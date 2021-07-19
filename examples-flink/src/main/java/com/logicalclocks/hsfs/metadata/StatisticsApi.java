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
import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.TrainingDataset;
import lombok.NonNull;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.logicalclocks.hsfs.metadata.HopsworksClient.PROJECT_PATH;
import static com.logicalclocks.hsfs.metadata.HopsworksClient.getInstance;

public class StatisticsApi {

  public static final String ENTITY_ROOT_PATH = "{/entityType}";
  public static final String ENTITY_ID_PATH = ENTITY_ROOT_PATH + "{/entityId}";
  public static final String STATISTICS_PATH = ENTITY_ID_PATH + "/statistics{?filter_by,fields,sort_by,offset,limit}";

  private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsApi.class);

  private EntityEndpointType entityType;

  public StatisticsApi(@NonNull EntityEndpointType entityType) {
    this.entityType = entityType;
  }

  public Statistics post(FeatureGroupBase featureGroup, Statistics statistics)
      throws FeatureStoreException, IOException {
    return post(featureGroup.getFeatureStore().getProjectId(), featureGroup.getFeatureStore().getId(),
        featureGroup.getId(), statistics);
  }

  public Statistics post(TrainingDataset trainingDataset, Statistics statistics)
      throws FeatureStoreException, IOException {
    return post(trainingDataset.getFeatureStore().getProjectId(), trainingDataset.getFeatureStore().getId(),
        trainingDataset.getId(), statistics);
  }

  private Statistics post(Integer projectId, Integer featurestoreId, Integer entityId, Statistics statistics)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = getInstance();
    String pathTemplate = PROJECT_PATH + FeatureStoreApi.FEATURE_STORE_PATH + STATISTICS_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", projectId)
        .set("fsId", featurestoreId)
        .set("entityType", entityType.getValue())
        .set("entityId", entityId)
        .expand();

    String statisticsJson = hopsworksClient.getObjectMapper().writeValueAsString(statistics);
    HttpPost postRequest = new HttpPost(uri);
    postRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    postRequest.setEntity(new StringEntity(statisticsJson));

    LOGGER.info("Sending metadata request: " + uri);
    LOGGER.info(statisticsJson);

    return hopsworksClient.handleRequest(postRequest, Statistics.class);
  }

  public Statistics get(FeatureGroupBase featureGroup, String commitTime) throws FeatureStoreException, IOException {
    return get(featureGroup.getFeatureStore().getProjectId(), featureGroup.getFeatureStore().getId(),
        featureGroup.getId(), commitTime);
  }

  public Statistics get(TrainingDataset trainingDataset, String commitTime) throws FeatureStoreException, IOException {
    return get(trainingDataset.getFeatureStore().getProjectId(), trainingDataset.getFeatureStore().getId(),
        trainingDataset.getId(), commitTime);
  }

  private Statistics get(Integer projectId, Integer featurestoreId, Integer entityId, String commitTime)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = getInstance();
    String pathTemplate = PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + STATISTICS_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", projectId)
        .set("fsId", featurestoreId)
        .set("entityType", entityType.getValue())
        .set("entityId", entityId)
        .set("filter_by", "commit_time_eq:" + commitTime)
        .set("fields", "content")
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    HttpGet getRequest = new HttpGet(uri);
    Statistics statistics = hopsworksClient.handleRequest(getRequest, Statistics.class);

    // currently getting multiple commits at the same time is not allowed
    if (statistics.getItems().size() == 1) {
      return statistics.getItems().get(0);
    }
    return null;
  }

  public Statistics getLast(FeatureGroupBase featureGroup) throws FeatureStoreException, IOException {
    return getLast(featureGroup.getFeatureStore().getProjectId(), featureGroup.getFeatureStore().getId(),
        featureGroup.getId());
  }

  public Statistics getLast(TrainingDataset trainingDataset) throws FeatureStoreException, IOException {
    return getLast(trainingDataset.getFeatureStore().getProjectId(), trainingDataset.getFeatureStore().getId(),
        trainingDataset.getId());
  }

  private Statistics getLast(Integer projectId, Integer featurestoreId, Integer entityId)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = getInstance();
    String pathTemplate = PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + STATISTICS_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", projectId)
        .set("fsId", featurestoreId)
        .set("entityType", entityType.getValue())
        .set("entityId", entityId)
        .set("sort_by", "commit_time:desc")
        .set("offset", 0)
        .set("limit", 1)
        .set("fields", "content")
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    HttpGet getRequest = new HttpGet(uri);
    Statistics statistics = hopsworksClient.handleRequest(getRequest, Statistics.class);

    // currently getting multiple commits at the same time is not allowed
    if (statistics.getItems().size() == 1) {
      return statistics.getItems().get(0);
    }
    return null;
  }

}
