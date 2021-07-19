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
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.logicalclocks.hsfs.metadata.HopsworksClient.PROJECT_PATH;
import static com.logicalclocks.hsfs.metadata.HopsworksClient.getInstance;

public class ExpectationsApi {

  public static final String ENTITY_ROOT_PATH = "{/entityType}";
  public static final String ENTITY_ID_PATH = ENTITY_ROOT_PATH + "{/entityId}";
  public static final String EXPECTATIONS_PATH =
      ENTITY_ID_PATH + "/expectations{/name}{?engine,filter_by,sort_by,offset,limit,expand}";

  private static final Logger LOGGER = LoggerFactory.getLogger(ExpectationsApi.class);

  private EntityEndpointType entityType;

  public ExpectationsApi() {
  }

  public ExpectationsApi(EntityEndpointType entityType) {
    this.entityType = entityType;
    LOGGER.info("ExpectationsApi.EXPECTATIONS_PATH:" + EXPECTATIONS_PATH);
  }

  public Expectation put(FeatureStore featureStore, Expectation expectation) throws FeatureStoreException, IOException {
    return put(featureStore.getProjectId(), featureStore.getId(), expectation);
  }

  public Expectation put(FeatureGroupBase featureGroupBase, String name) throws FeatureStoreException, IOException {
    return put(featureGroupBase.getFeatureStore().getProjectId(), featureGroupBase.getId(),
            featureGroupBase.getFeatureStore().getId(), name);
  }

  private Expectation put(Integer projectId, Integer featurestoreId, Expectation expectation)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = getInstance();
    String pathTemplate = PROJECT_PATH + FeatureStoreApi.FEATURE_STORE_PATH + EXPECTATIONS_PATH;
    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", projectId)
        .set("fsId", featurestoreId)
        .expand();

    String expectationStr = hopsworksClient.getObjectMapper().writeValueAsString(expectation);
    HttpPut putRequest = new HttpPut(uri);
    putRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    putRequest.setEntity(new StringEntity(expectationStr));

    LOGGER.info("Sending metadata request: " + uri);
    LOGGER.info(expectationStr);

    return hopsworksClient.handleRequest(putRequest, Expectation.class);
  }

  private Expectation put(Integer projectId, Integer entityId, Integer featurestoreId, String name)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = getInstance();
    String pathTemplate = PROJECT_PATH + FeatureStoreApi.FEATURE_STORE_PATH + EXPECTATIONS_PATH;
    LOGGER.info("pathTemplate: " + pathTemplate);
    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", projectId)
        .set("fsId", featurestoreId)
        .set("entityType", entityType.getValue())
        .set("entityId", entityId)
        .set("name", name)
        .expand();

    HttpPut putRequest = new HttpPut(uri);

    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(putRequest, Expectation.class);
  }

  public void detach(FeatureGroupBase featureGroupBase) throws FeatureStoreException, IOException {
    for (Expectation expectation : get(featureGroupBase)) {
      detach(featureGroupBase, expectation);
    }
  }

  public void detach(FeatureGroupBase featureGroupBase, Expectation expectation) throws FeatureStoreException,
                                                                                        IOException {
    delete(featureGroupBase.getFeatureStore().getProjectId(),
           featureGroupBase.getId(),
           featureGroupBase.getFeatureStore().getId(),
           expectation.getName());
  }

  /**
   * Detach an expectation from a feature group.
   * @param featureGroupBase feature group
   * @param name name of the expectation
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public void detach(FeatureGroupBase featureGroupBase, String name) throws FeatureStoreException, IOException {
    delete(featureGroupBase.getFeatureStore().getProjectId(), featureGroupBase.getId(),
           featureGroupBase.getFeatureStore().getId(),
           name);
  }

  public void delete(FeatureStore featureStore) throws FeatureStoreException, IOException {
    for (Expectation expectation : get(featureStore)) {
      delete(featureStore, expectation);
    }
  }

  public void delete(FeatureStore featureStore, Expectation expectation) throws FeatureStoreException, IOException {
    delete(featureStore.getProjectId(), null, featureStore.getId(), expectation.getName());
  }

  /**
   * Delete an expectation from the feature store.
   * @param featureStore featureStore
   * @param name name of the expectation
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public void delete(FeatureStore featureStore, String name) throws FeatureStoreException, IOException {
    delete(featureStore.getProjectId(), null, featureStore.getId(), name);
  }

  private void delete(Integer projectId, Integer entityId, Integer featurestoreId, String name)
      throws FeatureStoreException, IOException {
    String pathTemplate = PROJECT_PATH + FeatureStoreApi.FEATURE_STORE_PATH + EXPECTATIONS_PATH;
    LOGGER.info("pathTemplate: " + pathTemplate);
    UriTemplate uriTemplate = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", projectId)
        .set("fsId", featurestoreId)
        .set("name", name);

    if (entityId != null) {
      uriTemplate.set("entityType", entityType.getValue())
                 .set("entityId", entityId);
    }

    String uri = uriTemplate.expand();
    HttpDelete deleteRequest = new HttpDelete(uri);

    LOGGER.info("Sending metadata request: " + uri);
    HopsworksClient hopsworksClient = getInstance();
    hopsworksClient.handleRequest(deleteRequest);
  }

  public List<Expectation> get(FeatureStore featureStore) throws FeatureStoreException, IOException {
    return get(featureStore.getProjectId(), null, featureStore.getId(), null);
  }

  public Expectation get(FeatureStore featureStore, String name) throws FeatureStoreException, IOException {
    List<Expectation> expectations = get(featureStore.getProjectId(), null, featureStore.getId(), name);
    return !expectations.isEmpty() ? expectations.get(0) : null;
  }

  public List<Expectation> get(FeatureGroupBase featureGroupBase) throws FeatureStoreException, IOException {
    return get(featureGroupBase.getFeatureStore().getProjectId(), featureGroupBase.getId(),
            featureGroupBase.getFeatureStore().getId(), null);
  }

  /**
   * Used by PY5J.
   * @param projectId projectId
   * @param featuregroupId featuregroupId
   * @param featurestoreId featurestoreId
   * @return list of expectations
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public List<Expectation> get(Integer projectId, Integer featuregroupId, Integer featurestoreId)
      throws FeatureStoreException, IOException {
    return get(projectId, featuregroupId, featurestoreId, null);
  }

  public Expectation get(FeatureGroupBase featureGroupBase, String name) throws FeatureStoreException, IOException {
    List<Expectation> expectations =
        get(featureGroupBase.getFeatureStore().getProjectId(), null, featureGroupBase.getFeatureStore().getId(), name);
    return !expectations.isEmpty() ? expectations.get(0) : null;
  }

  private List<Expectation> get(Integer projectId, Integer entityId, Integer featurestoreId, String name)
      throws FeatureStoreException, IOException {
    String pathTemplate = PROJECT_PATH + FeatureStoreApi.FEATURE_STORE_PATH + EXPECTATIONS_PATH;

    UriTemplate uriTemplate = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", projectId)
        .set("fsId", featurestoreId)
        .set("expand","rules");

    if (entityId != null) {
      uriTemplate
          .set("entityType", entityType.getValue())
          .set("entityId", entityId);
    }

    if (name != null) {
      uriTemplate.set("name", name);
    }

    String uri = uriTemplate.expand();
    LOGGER.info("Sending metadata request: " + uri);
    HttpGet getRequest = new HttpGet(uri);
    HopsworksClient hopsworksClient = getInstance();
    Expectation dto = hopsworksClient.handleRequest(getRequest, Expectation.class);
    LOGGER.info("Received expectations dto: " + dto);
    List<Expectation> expectations;
    if (dto.getCount() == null) {
      expectations = new ArrayList<>();
      expectations.add(dto);
    } else {
      expectations = dto.getItems();
    }
    LOGGER.info("Received expectations: " + expectations);
    return expectations;
  }

}
