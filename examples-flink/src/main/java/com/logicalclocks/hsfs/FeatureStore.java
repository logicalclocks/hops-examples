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

package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.logicalclocks.hsfs.metadata.Expectation;
import com.logicalclocks.hsfs.metadata.ExpectationsApi;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.metadata.TrainingDatasetApi;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
/*
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
 */
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FeatureStore {

  @Getter
  @Setter
  @JsonProperty("featurestoreId")
  private Integer id;

  @Getter
  @Setter
  @JsonProperty("featurestoreName")
  private String name;

  @Getter
  @Setter
  private Integer projectId;

  private FeatureGroupApi featureGroupApi;
  private TrainingDatasetApi trainingDatasetApi;
  private StorageConnectorApi storageConnectorApi;
  private ExpectationsApi expectationsApi;

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureStore.class);

  private static final Integer DEFAULT_VERSION = 1;

  public FeatureStore() {
    featureGroupApi = new FeatureGroupApi();
    trainingDatasetApi = new TrainingDatasetApi();
    storageConnectorApi = new StorageConnectorApi();
    expectationsApi = new ExpectationsApi();
  }

  /**
   * Get a feature group object from the feature store.
   *
   * @param name    the name of the feature group
   * @param version the version of the feature group
   * @return FeatureGroup
   * @throws FeatureStoreException
   * @throws IOException
   */
  public FeatureGroup getFeatureGroup(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return featureGroupApi.getFeatureGroup(this, name, version);
  }

  /**
   * Get a feature group object with default version `1` from the feature store.
   *
   * @param name the name of the feature group
   * @return FeatureGroup
   * @throws FeatureStoreException
   * @throws IOException
   */
  public FeatureGroup getFeatureGroup(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting feature group `" + name + "`, defaulting to `"
        + DEFAULT_VERSION + "`.");
    return getFeatureGroup(name, DEFAULT_VERSION);
  }

  /**
   * Get a on-demand feature group object from the feature store.
   *
   * @param name    the name of the feature group
   * @param version the version of the feature group
   * @return OnDemandFeatureGroup
   * @throws FeatureStoreException
   * @throws IOException
   */
  public OnDemandFeatureGroup getOnDemandFeatureGroup(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return featureGroupApi.getOnDemandFeatureGroup(this, name, version);
  }

  /**
   * Get a on-demand feature group object with default version `1` from the feature store.
   *
   * @param name the name of the feature group
   * @return OnDemandFeatureGroup
   * @throws FeatureStoreException
   * @throws IOException
   */
  public OnDemandFeatureGroup getOnDemandFeatureGroup(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting feature group `" + name + "`, defaulting to `"
        + DEFAULT_VERSION + "`.");
    return getOnDemandFeatureGroup(name, DEFAULT_VERSION);
  }

  /*
  public Dataset<Row> sql(String query) {
    return SparkEngine.getInstance().sql(query);
  }
   */

  public StorageConnector getStorageConnector(String name) throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name);
  }

  public StorageConnector.JdbcConnector getJdbcConnector(String name) throws FeatureStoreException, IOException {
    return (StorageConnector.JdbcConnector) storageConnectorApi.getByName(this, name);
  }

  public StorageConnector.S3Connector getS3Connector(String name) throws FeatureStoreException, IOException {
    return (StorageConnector.S3Connector) storageConnectorApi.getByName(this, name);
  }

  public StorageConnector.HopsFsConnector getHopsFsConnector(String name) throws FeatureStoreException, IOException {
    return (StorageConnector.HopsFsConnector) storageConnectorApi.getByName(this, name);
  }

  public StorageConnector.RedshiftConnector getRedshiftConnector(String name)
      throws FeatureStoreException, IOException {
    return (StorageConnector.RedshiftConnector) storageConnectorApi.getByName(this, name);
  }

  public StorageConnector.SnowflakeConnector getSnowflakeConnector(String name)
      throws FeatureStoreException, IOException {
    return (StorageConnector.SnowflakeConnector) storageConnectorApi.getByName(this, name);
  }

  public StorageConnector.AdlsConnector getAdlsConnector(String name) throws FeatureStoreException, IOException {
    return (StorageConnector.AdlsConnector) storageConnectorApi.getByName(this, name);
  }

  public StorageConnector.JdbcConnector getOnlineStorageConnector() throws FeatureStoreException, IOException {
    return storageConnectorApi.getOnlineStorageConnector(this);
  }

  public FeatureGroup.FeatureGroupBuilder createFeatureGroup() {
    return FeatureGroup.builder()
        .featureStore(this);
  }

  public OnDemandFeatureGroup.OnDemandFeatureGroupBuilder createOnDemandFeatureGroup() {
    return OnDemandFeatureGroup.builder()
        .featureStore(this);
  }

  public TrainingDataset.TrainingDatasetBuilder createTrainingDataset() {
    return TrainingDataset.builder()
        .featureStore(this);
  }


  public Expectation.ExpectationBuilder createExpectation() {
    return Expectation.builder()
        .featureStore(this);
  }

  /**
   * Get a training dataset object from the selected feature store.
   *
   * @param name    name of the training dataset
   * @param version version to get
   * @return TrainingDataset
   * @throws FeatureStoreException
   * @throws IOException
   */
  public TrainingDataset getTrainingDataset(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return trainingDatasetApi.get(this, name, version);
  }

  /**
   * Get a training dataset object with the default version `1` from the selected feature store.
   *
   * @param name name of the training dataset
   * @return TrainingDataset
   * @throws FeatureStoreException
   * @throws IOException
   */
  public TrainingDataset getTrainingDataset(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting training dataset `" + name + "`, defaulting to `"
        + DEFAULT_VERSION + "`.");
    return getTrainingDataset(name, DEFAULT_VERSION);
  }

  public scala.collection.Seq<Expectation> createExpectations(scala.collection.Seq<Expectation> expectations)
      throws FeatureStoreException, IOException {
    List<Expectation> newExpectations = new ArrayList<>();
    List<Expectation> expectationsList =
        (List<Expectation>) JavaConverters.seqAsJavaListConverter(expectations).asJava();
    for (Expectation expectation :  expectationsList) {
      expectation = expectationsApi.put(this, expectation);
      newExpectations.add(expectation);
    }
    return JavaConverters.asScalaBufferConverter(newExpectations).asScala().toSeq();
  }

  public Expectation getExpectation(String name)
      throws FeatureStoreException, IOException {
    return expectationsApi.get(this, name);
  }

  public scala.collection.Seq<Expectation> getExpectations() throws FeatureStoreException, IOException {
    return JavaConverters.asScalaBufferConverter(expectationsApi.get(this)).asScala().toSeq();
  }

  public void deleteExpectation(Expectation expectation) throws FeatureStoreException, IOException {
    deleteExpectation(expectation.getName());
  }

  public void deleteExpectation(String name) throws FeatureStoreException, IOException {
    expectationsApi.delete(this, name);
  }

  public void deleteExpectations(scala.collection.Seq<Expectation> expectations)
      throws FeatureStoreException, IOException {
    for (Expectation expectation :  (List<Expectation>) JavaConverters.seqAsJavaListConverter(expectations).asJava()) {
      deleteExpectation(expectation);
    }
  }

  @Override
  public String toString() {
    return "FeatureStore{"
        + "id=" + id
        + ", name='" + name + '\''
        + ", projectId=" + projectId
        + ", featureGroupApi=" + featureGroupApi
        + '}';
  }
}
