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

package com.logicalclocks.hsfs.metadata;

import com.damnhandy.uri.template.UriTemplate;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.constructor.FsQuery;
import com.logicalclocks.hsfs.TrainingDataset;
import com.logicalclocks.hsfs.constructor.ServingPreparedStatement;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static com.logicalclocks.hsfs.metadata.HopsworksClient.PROJECT_PATH;

public class TrainingDatasetApi {

  private static final String TRAINING_DATASETS_PATH = "/trainingdatasets";
  private static final String TRAINING_DATASET_PATH = TRAINING_DATASETS_PATH + "{/tdName}{?version}";
  private static final String TRAINING_QUERY_PATH = TRAINING_DATASETS_PATH + "{/tdId}/query{?withLabel}";
  public static final String TRAINING_DATASET_ID_PATH = TRAINING_DATASETS_PATH + "{/fgId}{?updateStatsConfig,"
      + "updateMetadata}";
  private static final String PREP_STATEMENT_PATH = TRAINING_DATASETS_PATH + "{/tdId}/preparedstatements";
  private static final String TRANSFORMATION_FUNCTION_PATH =
      TRAINING_DATASETS_PATH + "{/tdId}/transformationfunctions";

  private static final Logger LOGGER = LoggerFactory.getLogger(TrainingDatasetApi.class);

  public TrainingDataset get(FeatureStore featureStore, String tdName, Integer tdVersion)
      throws IOException, FeatureStoreException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + TRAINING_DATASET_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureStore.getProjectId())
        .set("fsId", featureStore.getId())
        .set("tdName", tdName)
        .set("version", tdVersion)
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    TrainingDataset[] trainingDatasets = hopsworksClient.handleRequest(new HttpGet(uri), TrainingDataset[].class);

    // There can be only one single training dataset with a specific name and version in a feature store
    // There has to be one otherwise an exception would have been thrown.
    TrainingDataset resultTd = trainingDatasets[0];
    resultTd.setFeatureStore(featureStore);
    resultTd.getFeatures().stream()
        .filter(f -> f.getFeaturegroup() != null)
        .forEach(f -> f.getFeaturegroup().setFeatureStore(featureStore));
    return resultTd;
  }

  public TrainingDataset createTrainingDataset(TrainingDataset trainingDataset)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + TRAINING_DATASETS_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", trainingDataset.getFeatureStore().getProjectId())
        .set("fsId", trainingDataset.getFeatureStore().getId())
        .expand();

    String trainingDatasetJson = hopsworksClient.getObjectMapper().writeValueAsString(trainingDataset);
    HttpPost postRequest = new HttpPost(uri);
    postRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    postRequest.setEntity(new StringEntity(trainingDatasetJson));

    LOGGER.info("Sending metadata request: " + uri);
    LOGGER.info(trainingDatasetJson);
    return hopsworksClient.handleRequest(postRequest, TrainingDataset.class);
  }

  public FsQuery getQuery(TrainingDataset trainingDataset, boolean withLabel)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + TRAINING_QUERY_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", trainingDataset.getFeatureStore().getProjectId())
        .set("fsId", trainingDataset.getFeatureStore().getId())
        .set("tdId", trainingDataset.getId())
        .set("withLabel", withLabel)
        .expand();

    HttpGet getRequest = new HttpGet(uri);
    LOGGER.info("Sending metadata request: " + uri);

    return hopsworksClient.handleRequest(getRequest, FsQuery.class);
  }

  public List<ServingPreparedStatement> getServingPreparedStatement(TrainingDataset trainingDataset)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + PREP_STATEMENT_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", trainingDataset.getFeatureStore().getProjectId())
        .set("fsId", trainingDataset.getFeatureStore().getId())
        .set("tdId", trainingDataset.getId())
        .expand();

    HttpGet getRequest = new HttpGet(uri);
    LOGGER.info("Sending metadata request: " + uri);

    ServingPreparedStatement servingPreparedStatement = hopsworksClient.handleRequest(getRequest,
        ServingPreparedStatement.class);
    return servingPreparedStatement.getItems();
  }

  public TrainingDataset updateMetadata(TrainingDataset trainingDataset, String queryParameter)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + TRAINING_DATASET_ID_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", trainingDataset.getFeatureStore().getProjectId())
        .set("fsId", trainingDataset.getFeatureStore().getId())
        .set("fgId", trainingDataset.getId())
        .set(queryParameter, true)
        .expand();

    String trainingDatasetJson = hopsworksClient.getObjectMapper().writeValueAsString(trainingDataset);
    HttpPut putRequest = new HttpPut(uri);
    putRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    putRequest.setEntity(new StringEntity(trainingDatasetJson));

    LOGGER.info("Sending metadata request: " + uri);
    LOGGER.info(trainingDatasetJson);

    return hopsworksClient.handleRequest(putRequest, TrainingDataset.class);
  }

  public void delete(TrainingDataset trainingDataset)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + TRAINING_DATASET_ID_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", trainingDataset.getFeatureStore().getProjectId())
        .set("fsId", trainingDataset.getFeatureStore().getId())
        .set("fgId", trainingDataset.getId())
        .expand();

    HttpDelete deleteRequest = new HttpDelete(uri);
    LOGGER.info("Sending metadata request: " + uri);

    hopsworksClient.handleRequest(deleteRequest);
  }

  public TrainingDataset getTransformationFunctions(TrainingDataset trainingDataset)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + TRANSFORMATION_FUNCTION_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", trainingDataset.getFeatureStore().getProjectId())
        .set("fsId", trainingDataset.getFeatureStore().getId())
        .set("tdId", trainingDataset.getId())
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(new HttpGet(uri), TrainingDataset.class);
  }
}
