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

import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.StorageConnectorType;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.TrainingDatasetType;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
/*
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.col;
*/
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Utils {

  StorageConnectorApi storageConnectorApi = new StorageConnectorApi();

  /*
  public List<Feature> parseFeatureGroupSchema(Dataset<Row> dataset) throws FeatureStoreException {
    List<Feature> features = new ArrayList<>();
    for (StructField structField : dataset.schema().fields()) {
      // TODO(Fabio): unit test this one for complext types
      Feature f = new Feature(structField.name().toLowerCase(), structField.dataType().catalogString(), false, false);
      if (structField.metadata().contains("description")) {
        f.setDescription(structField.metadata().getString("description"));
      }
      features.add(f);
    }

    return features;
  }

  public List<TrainingDatasetFeature> parseTrainingDatasetSchema(Dataset<Row> dataset) throws FeatureStoreException {
    List<TrainingDatasetFeature> features = new ArrayList<>();

    int index = 0;
    for (StructField structField : dataset.schema().fields()) {
      // TODO(Fabio): unit test this one for complext types
      features.add(new TrainingDatasetFeature(
          structField.name().toLowerCase(), structField.dataType().catalogString(), index++));
    }

    return features;
  }

  public Dataset<Row> sanitizeFeatureNames(Dataset<Row> dataset) {
    return dataset.select(Arrays.asList(dataset.columns()).stream().map(f -> col(f).alias(f.toLowerCase())).toArray(
        Column[]::new));
  }

  public void trainingDatasetSchemaMatch(Dataset<Row> dataset, List<TrainingDatasetFeature> features)
      throws FeatureStoreException {
    StructType tdStructType = new StructType(features.stream()
        .sorted(Comparator.comparingInt(TrainingDatasetFeature::getIndex))
        .map(f -> new StructField(f.getName(),
            // What should we do about the nullables
            new CatalystSqlParser().parseDataType(f.getType()), true, Metadata.empty())
        ).toArray(StructField[]::new));

    if (!dataset.schema().equals(tdStructType)) {
      throw new FeatureStoreException("The Dataframe schema: " + dataset.schema()
          + " does not match the training dataset schema: " + tdStructType);
    }
  }
  */

  public TrainingDatasetType getTrainingDatasetType(StorageConnector storageConnector) {
    if (storageConnector == null) {
      return TrainingDatasetType.HOPSFS_TRAINING_DATASET;
    } else if (storageConnector.getStorageConnectorType() == StorageConnectorType.HOPSFS) {
      return TrainingDatasetType.HOPSFS_TRAINING_DATASET;
    } else {
      return TrainingDatasetType.EXTERNAL_TRAINING_DATASET;
    }
  }

  // TODO(Fabio): this should be moved in the backend
  public String getTableName(FeatureGroup offlineFeatureGroup) {
    return offlineFeatureGroup.getFeatureStore().getName() + "."
        + offlineFeatureGroup.getName() + "_" + offlineFeatureGroup.getVersion();
  }

  public String getOnlineTableName(FeatureGroup offlineFeatureGroup) {
    return offlineFeatureGroup.getName() + "_" + offlineFeatureGroup.getVersion();
  }

  public Seq<String> getPartitionColumns(FeatureGroup offlineFeatureGroup) {
    List<String> partitionCols = offlineFeatureGroup.getFeatures().stream()
        .filter(Feature::getPartition)
        .map(Feature::getName)
        .collect(Collectors.toList());

    return JavaConverters.asScalaIteratorConverter(partitionCols.iterator()).asScala().toSeq();
  }

  public Seq<String> getPrimaryColumns(FeatureGroup offlineFeatureGroup) {
    List<String> primaryCols = offlineFeatureGroup.getFeatures().stream()
        .filter(Feature::getPrimary)
        .map(Feature::getName)
        .collect(Collectors.toList());

    return JavaConverters.asScalaIteratorConverter(primaryCols.iterator()).asScala().toSeq();
  }

  public String getFgName(FeatureGroup featureGroup) {
    return featureGroup.getName() + "_" + featureGroup.getVersion();
  }

  public String getHiveServerConnection(FeatureGroup featureGroup) throws IOException, FeatureStoreException {
    Map<String, String> credentials = new HashMap<>();
    credentials.put("sslTrustStore", HopsworksClient.getInstance().getHopsworksHttpClient().getTrustStorePath());
    credentials.put("trustStorePassword", HopsworksClient.getInstance().getHopsworksHttpClient().getCertKey());
    credentials.put("sslKeyStore", HopsworksClient.getInstance().getHopsworksHttpClient().getKeyStorePath());
    credentials.put("keyStorePassword", HopsworksClient.getInstance().getHopsworksHttpClient().getCertKey());

    StorageConnector.JdbcConnector storageConnector =
        (StorageConnector.JdbcConnector) storageConnectorApi.getByName(featureGroup.getFeatureStore(),
            featureGroup.getFeatureStore().getName());

    return storageConnector.getConnectionString()
        + credentials.entrySet().stream().map(cred -> cred.getKey() + "=" + cred.getValue())
        .collect(Collectors.joining(";"));
  }

  public Long getTimeStampFromDateString(String inputDate) throws FeatureStoreException, ParseException {

    HashMap<Pattern, String> dateFormatPatterns = new HashMap<Pattern, String>() {{
          put(Pattern.compile("^([0-9]{4})([0-9]{2})([0-9]{2})$"), "yyyyMMdd");
          put(Pattern.compile("^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})$"), "yyyyMMddHH");
          put(Pattern.compile("^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})$"), "yyyyMMddHHmm");
          put(Pattern.compile("^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})$"), "yyyyMMddHHmmss");
      }};

    String tempDate = inputDate.replace("/", "")
        .replace("-", "").replace(" ", "")
        .replace(":","");
    String dateFormatPattern = null;

    for (Pattern pattern : dateFormatPatterns.keySet()) {
      if (pattern.matcher(tempDate).matches()) {
        dateFormatPattern = dateFormatPatterns.get(pattern);
        break;
      }
    }

    if (dateFormatPattern == null) {
      throw new FeatureStoreException("Unable to identify format of the provided date value : " + inputDate);
    }

    SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatPattern);
    Long commitTimeStamp = dateFormat.parse(tempDate).getTime();;

    return commitTimeStamp;
  }
}
