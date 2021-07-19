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
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureGroupCommit;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.OnDemandFeatureGroup;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static com.logicalclocks.hsfs.metadata.HopsworksClient.PROJECT_PATH;

public class FeatureGroupApi {

  public static final String FEATURE_GROUP_ROOT_PATH = "/featuregroups";
  public static final String FEATURE_GROUP_PATH = FEATURE_GROUP_ROOT_PATH + "{/fgName}{?version}";
  public static final String FEATURE_GROUP_ID_PATH = FEATURE_GROUP_ROOT_PATH + "{/fgId}{?updateStatsConfig,"
      + "updateMetadata,validationType}";
  public static final String FEATURE_GROUP_COMMIT_PATH = FEATURE_GROUP_ID_PATH
      + "/commits{?filter_by,sort_by,offset,limit}";
  public static final String FEATURE_GROUP_CLEAR_PATH = FEATURE_GROUP_ID_PATH + "/clear";

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroupApi.class);

  public FeatureGroup getFeatureGroup(FeatureStore featureStore, String fgName, Integer fgVersion)
      throws IOException, FeatureStoreException {
    FeatureGroup[] offlineFeatureGroups =
        getInternal(featureStore, fgName, fgVersion, FeatureGroup[].class);

    // There can be only one single feature group with a specific name and version in a feature store
    // There has to be one otherwise an exception would have been thrown.
    FeatureGroup resultFg = offlineFeatureGroups[0];
    resultFg.setFeatureStore(featureStore);
    return resultFg;
  }

  public OnDemandFeatureGroup getOnDemandFeatureGroup(FeatureStore featureStore, String fgName, Integer fgVersion)
      throws IOException, FeatureStoreException {
    OnDemandFeatureGroup[] offlineFeatureGroups =
        getInternal(featureStore, fgName, fgVersion, OnDemandFeatureGroup[].class);

    // There can be only one single feature group with a specific name and version in a feature store
    // There has to be one otherwise an exception would have been thrown.
    OnDemandFeatureGroup resultFg = offlineFeatureGroups[0];
    resultFg.setFeatureStore(featureStore);
    return resultFg;
  }

  private <T> T getInternal(FeatureStore featureStore, String fgName, Integer fgVersion, Class<T> fgType)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureStore.getProjectId())
        .set("fsId", featureStore.getId())
        .set("fgName", fgName)
        .set("version", fgVersion)
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(new HttpGet(uri), fgType);
  }

  public OnDemandFeatureGroup save(OnDemandFeatureGroup onDemandFeatureGroup)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String featureGroupJson = hopsworksClient.getObjectMapper().writeValueAsString(onDemandFeatureGroup);

    return saveInternal(onDemandFeatureGroup, new StringEntity(featureGroupJson), OnDemandFeatureGroup.class);
  }

  public FeatureGroup save(FeatureGroup featureGroup) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String featureGroupJson = hopsworksClient.getObjectMapper().writeValueAsString(featureGroup);

    return saveInternal(featureGroup, new StringEntity(featureGroupJson), FeatureGroup.class);
  }

  private <T> T saveInternal(FeatureGroupBase featureGroupBase,
                             StringEntity entity, Class<T> fgType) throws FeatureStoreException, IOException {
    String pathTemplate = PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_ROOT_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureGroupBase.getFeatureStore().getProjectId())
        .set("fsId", featureGroupBase.getFeatureStore().getId())
        .expand();

    HttpPost postRequest = new HttpPost(uri);
    postRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    postRequest.setEntity(entity);

    LOGGER.info("Sending metadata request: " + uri);

    return HopsworksClient.getInstance().handleRequest(postRequest, fgType);
  }

  public void delete(FeatureGroupBase featureGroupBase) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_ID_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureGroupBase.getFeatureStore().getProjectId())
        .set("fsId", featureGroupBase.getFeatureStore().getId())
        .set("fgId", featureGroupBase.getId())
        .expand();

    HttpDelete deleteRequest = new HttpDelete(uri);

    LOGGER.info("Sending metadata request: " + uri);
    hopsworksClient.handleRequest(deleteRequest);
  }

  public void deleteContent(FeatureGroup featureGroup) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_CLEAR_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureGroup.getFeatureStore().getProjectId())
        .set("fsId", featureGroup.getFeatureStore().getId())
        .set("fgId", featureGroup.getId())
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    HttpPost postRequest = new HttpPost(uri);
    hopsworksClient.handleRequest(postRequest);
  }

  public FeatureGroup updateMetadata(FeatureGroupBase featureGroup, String queryParameter)
      throws FeatureStoreException, IOException {
    return updateMetadata(featureGroup, queryParameter, true);
  }

  public FeatureGroup updateMetadata(FeatureGroupBase featureGroup, String queryParameter, Object value)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_ID_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureGroup.getFeatureStore().getProjectId())
        .set("fsId", featureGroup.getFeatureStore().getId())
        .set("fgId", featureGroup.getId())
        .set(queryParameter, value)
        .expand();

    String featureGroupJson = hopsworksClient.getObjectMapper().writeValueAsString(featureGroup);
    HttpPut putRequest = new HttpPut(uri);
    putRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    putRequest.setEntity(new StringEntity(featureGroupJson));

    LOGGER.info("Sending metadata request: " + uri);
    LOGGER.info(featureGroupJson);

    return hopsworksClient.handleRequest(putRequest, FeatureGroup.class);
  }

  public FeatureGroupCommit featureGroupCommit(FeatureGroup featureGroup, FeatureGroupCommit featureGroupCommit)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_COMMIT_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureGroup.getFeatureStore().getProjectId())
        .set("fsId", featureGroup.getFeatureStore().getId())
        .set("fgId", featureGroup.getId())
        .expand();

    String featureGroupCommitJson = hopsworksClient.getObjectMapper().writeValueAsString(featureGroupCommit);
    HttpPost postRequest = new HttpPost(uri);
    postRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    postRequest.setEntity(new StringEntity(featureGroupCommitJson));

    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(postRequest, FeatureGroupCommit.class);
  }

  public List<FeatureGroupCommit> getCommitDetails(FeatureGroup featureGroupBase, Long wallclockTimestamp,
                                                   Integer limit) throws IOException, FeatureStoreException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_COMMIT_PATH;

    UriTemplate uriTemplate = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureGroupBase.getFeatureStore().getProjectId())
        .set("fsId", featureGroupBase.getFeatureStore().getId())
        .set("fgId", featureGroupBase.getId())
        .set("sort_by", "committed_on:desc")
        .set("offset", 0)
        .set("limit", limit);

    if (wallclockTimestamp != null) {
      uriTemplate.set("filter_by", "commited_on_ltoeq:" + wallclockTimestamp);
    }

    String uri = uriTemplate.expand();

    LOGGER.info("Sending metadata request: " + uri);
    FeatureGroupCommit featureGroupCommit = hopsworksClient.handleRequest(new HttpGet(uri), FeatureGroupCommit.class);
    return featureGroupCommit.getItems();
  }
}
