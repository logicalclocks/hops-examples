/*
 * Copyright (c) 2020 Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.TrainingDataset;
import com.logicalclocks.hsfs.TrainingDatasetFeature;
import com.logicalclocks.hsfs.constructor.ServingPreparedStatement;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.metadata.TagsApi;
import com.logicalclocks.hsfs.metadata.TrainingDatasetApi;
import com.logicalclocks.hsfs.util.Constants;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
/*
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
*/
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import java.sql.DriverManager;
import java.util.TreeMap;

public class TrainingDatasetEngine {

  private TrainingDatasetApi trainingDatasetApi = new TrainingDatasetApi();
  private TagsApi tagsApi = new TagsApi(EntityEndpointType.TRAINING_DATASET);
  private StorageConnectorApi storageConnectorApi = new StorageConnectorApi();
  private Utils utils = new Utils();
  private Schema.Parser parser = new Schema.Parser();
  private BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(new byte[0], null);

  private static final Logger LOGGER = LoggerFactory.getLogger(TrainingDatasetEngine.class);

  public void addTag(TrainingDataset trainingDataset, String name, Object value)
      throws FeatureStoreException, IOException {
    tagsApi.add(trainingDataset, name, value);
  }

  public Map<String, Object> getTags(TrainingDataset trainingDataset) throws FeatureStoreException, IOException {
    return tagsApi.get(trainingDataset);
  }

  public Object getTag(TrainingDataset trainingDataset, String name) throws FeatureStoreException, IOException {
    return tagsApi.get(trainingDataset, name);
  }

  public void deleteTag(TrainingDataset trainingDataset, String name) throws FeatureStoreException, IOException {
    tagsApi.deleteTag(trainingDataset, name);
  }

  public String getQuery(TrainingDataset trainingDataset, Storage storage, boolean withLabel)
      throws FeatureStoreException, IOException {
    return trainingDatasetApi.getQuery(trainingDataset, withLabel).getStorageQuery(storage);
  }

  public void updateStatisticsConfig(TrainingDataset trainingDataset) throws FeatureStoreException, IOException {
    TrainingDataset apiTD = trainingDatasetApi.updateMetadata(trainingDataset, "updateStatsConfig");
    trainingDataset.getStatisticsConfig().setCorrelations(apiTD.getStatisticsConfig().getCorrelations());
    trainingDataset.getStatisticsConfig().setHistograms(apiTD.getStatisticsConfig().getHistograms());
  }

  public void initPreparedStatement(TrainingDataset trainingDataset, boolean external)
      throws FeatureStoreException, IOException, SQLException {

    // check if this training dataset has transformation functions attached and throw exception if any
    if (getTransformationFunctions(trainingDataset).size() > 0) {
      throw new FeatureStoreException("This training dataset has transformation functions attached and "
          + "serving must performed from a Python application");
    }

    StorageConnector storageConnector =
        storageConnectorApi.getOnlineStorageConnector(trainingDataset.getFeatureStore());
    Map<String, String> jdbcOptions = storageConnector.sparkOptions();
    String url = jdbcOptions.get(Constants.JDBC_URL);
    if (external) {
      // if external is true, replace the IP coming from the storage connector with the host
      // used during the connection setup
      url = url.replaceAll("/[0-9.]+:", "/" + HopsworksClient.getInstance().getHost() + ":");
    }
    Connection jdbcConnection =
        DriverManager.getConnection(url, jdbcOptions.get(Constants.JDBC_USER), jdbcOptions.get(Constants.JDBC_PWD));
    jdbcConnection.setAutoCommit(false);
    trainingDataset.setPreparedStatementConnection(jdbcConnection);

    List<ServingPreparedStatement> servingPreparedStatements =
        trainingDatasetApi.getServingPreparedStatement(trainingDataset);
    // map of prepared statement index and its corresponding parameter indices
    Map<Integer, Map<String, Integer>> preparedStatementParameters = new HashMap<>();
    // save map of fg index and its prepared statement
    TreeMap<Integer, PreparedStatement> preparedStatements = new TreeMap<>();
    // save unique primary key names that will be used by user to retrieve serving vector
    HashSet<String> servingVectorKeys = new HashSet<>();
    for (ServingPreparedStatement servingPreparedStatement: servingPreparedStatements) {
      preparedStatements.put(servingPreparedStatement.getPreparedStatementIndex(),
          jdbcConnection.prepareStatement(servingPreparedStatement.getQueryOnline()));
      HashMap<String, Integer> parameterIndices = new HashMap<>();
      servingPreparedStatement.getPreparedStatementParameters().forEach(preparedStatementParameter -> {
          servingVectorKeys.add(preparedStatementParameter.getName());
          parameterIndices.put(preparedStatementParameter.getName(), preparedStatementParameter.getIndex());
        });
      preparedStatementParameters.put(servingPreparedStatement.getPreparedStatementIndex(), parameterIndices);
    }
    trainingDataset.setServingKeys(servingVectorKeys);
    trainingDataset.setPreparedStatementParameters(preparedStatementParameters);
    trainingDataset.setPreparedStatements(preparedStatements);
  }

  public List<Object> getServingVector(TrainingDataset trainingDataset, Map<String, Object> entry, boolean external)
      throws SQLException, FeatureStoreException, IOException {

    // init prepared statement if it has not already
    if (trainingDataset.getPreparedStatements() == null) {
      initPreparedStatement(trainingDataset, external);
    }
    //check if primary key map correspond to serving_keys.
    if (!trainingDataset.getServingKeys().equals(entry.keySet())) {
      throw new IllegalArgumentException("Provided primary key map doesn't correspond to serving_keys");
    }

    Map<Integer, Map<String, Integer>> preparedStatementParameters = trainingDataset.getPreparedStatementParameters();
    TreeMap<Integer, PreparedStatement> preparedStatements = trainingDataset.getPreparedStatements();
    Map<String, DatumReader<Object>> complexFeatureSchemas = getComplexFeatureSchemas(trainingDataset);

    // Iterate over entry map of preparedStatements and set values to them
    for (Integer fgId : preparedStatements.keySet()) {
      Map<String, Integer> parameterIndexInStatement = preparedStatementParameters.get(fgId);
      for (String name : entry.keySet()) {
        if (parameterIndexInStatement.containsKey(name)) {
          preparedStatements.get(fgId).setObject(parameterIndexInStatement.get(name), entry.get(name));
        }
      }
    }

    // construct serving vector
    ArrayList<Object> servingVector = new ArrayList<>();
    for (Integer preparedStatementIndex : preparedStatements.keySet()) {
      ResultSet results = preparedStatements.get(preparedStatementIndex).executeQuery();
      // check if results contain any data at all and throw exception if not
      if (!results.isBeforeFirst()) {
        throw new FeatureStoreException("No data was retrieved from online feature store using input " + entry);
      }
      //Get column count
      int columnCount = results.getMetaData().getColumnCount();
      //append results to servingVector
      while (results.next()) {
        int index = 1;
        while (index <= columnCount) {
          if (complexFeatureSchemas.containsKey(results.getMetaData().getColumnName(index))) {
            servingVector.add(deserializeComplexFeature(complexFeatureSchemas, results, index));
          } else {
            servingVector.add(results.getObject(index));
          }
          index++;
        }
      }
      results.close();
    }
    trainingDataset.getPreparedStatementConnection().commit();
    return servingVector;
  }

  private Object deserializeComplexFeature(Map<String, DatumReader<Object>> complexFeatureSchemas, ResultSet results,
      int index) throws SQLException, IOException {
    Decoder decoder = DecoderFactory.get().binaryDecoder(results.getBytes(index), binaryDecoder);
    return complexFeatureSchemas.get(results.getMetaData().getColumnName(index)).read(null, decoder);
  }

  private Map<String, DatumReader<Object>> getComplexFeatureSchemas(TrainingDataset trainingDataset)
      throws FeatureStoreException, IOException {
    Map<String, DatumReader<Object>> featureSchemaMap = new HashMap<>();
    for (TrainingDatasetFeature f : trainingDataset.getFeatures()) {
      if (f.isComplex()) {
        DatumReader<Object> datumReader =
            new GenericDatumReader<>(parser.parse(f.getFeaturegroup().getFeatureAvroSchema(f.getName())));
        featureSchemaMap.put(f.getName(), datumReader);
      }
    }
    return featureSchemaMap;
  }

  public void delete(TrainingDataset trainingDataset) throws FeatureStoreException, IOException {
    trainingDatasetApi.delete(trainingDataset);
  }

  private List<TrainingDatasetFeature> getTransformationFunctions(TrainingDataset trainingDataset)
      throws FeatureStoreException, IOException {
    List<TrainingDatasetFeature> featuresWithtransformationFunction = new ArrayList<>();

    TrainingDataset updatedTrainingDataset =
        trainingDatasetApi.getTransformationFunctions(trainingDataset);

    for (TrainingDatasetFeature trainingDatasetFeature: updatedTrainingDataset.getFeatures()) {
      if (trainingDatasetFeature.getTransformationFunction() != null) {
        featuresWithtransformationFunction.add(trainingDatasetFeature);
      }
    }
    return featuresWithtransformationFunction;
  }
}
