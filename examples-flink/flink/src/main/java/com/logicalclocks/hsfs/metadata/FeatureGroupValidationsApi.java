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
import com.logicalclocks.hsfs.engine.DataValidationEngine;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.http.HttpHeaders;
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

public class FeatureGroupValidationsApi {

  public static final String ENTITY_ROOT_PATH = "{/entityType}";
  public static final String ENTITY_ID_PATH = ENTITY_ROOT_PATH + "{/entityId}";
  public static final String RESULTS_PATH =
      ENTITY_ID_PATH + "/validations{/id}{?filter_by,sort_by,offset,limit}";

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroupValidationsApi.class);

  private final EntityEndpointType entityType;

  public FeatureGroupValidationsApi(@NonNull EntityEndpointType entityType) {
    this.entityType = entityType;
  }

  public List<FeatureGroupValidation> get(FeatureGroupBase featureGroupBase)
      throws FeatureStoreException, IOException {
    return get(featureGroupBase.getFeatureStore().getProjectId(), featureGroupBase.getFeatureStore().getId(),
               featureGroupBase.getId(), null);
  }

  public FeatureGroupValidation get(FeatureGroupBase featureGroupBase,
      ImmutablePair<DataValidationEngine.ValidationTimeType, Long> pair) throws FeatureStoreException, IOException {
    return get(featureGroupBase.getFeatureStore().getProjectId(), featureGroupBase.getFeatureStore().getId(),
            featureGroupBase.getId(), pair).get(0);
  }

  private List<FeatureGroupValidation> get(Integer projectId, Integer featurestoreId, Integer entityId,
      ImmutablePair<DataValidationEngine.ValidationTimeType, Long> pair)
      throws FeatureStoreException, IOException {

    String pathTemplate = PROJECT_PATH + FeatureStoreApi.FEATURE_STORE_PATH + RESULTS_PATH;

    UriTemplate uriTemplate = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", projectId)
        .set("fsId", featurestoreId)
        .set("entityType", entityType.getValue())
        .set("entityId", entityId);
    if (pair != null) {
      if (pair.getLeft() == DataValidationEngine.ValidationTimeType.VALIDATION_TIME) {
        uriTemplate.set("filter_by", "validation_time_eq:" + pair.getRight());
      } else if (pair.getLeft() == DataValidationEngine.ValidationTimeType.COMMIT_TIME) {
        uriTemplate.set("filter_by", "commit_time_eq:" + pair.getRight());
      }
    }

    String uri = uriTemplate.expand();
    LOGGER.info("Sending metadata request: " + uri);
    HttpGet getRequest = new HttpGet(uri);
    HopsworksClient hopsworksClient = getInstance();
    FeatureGroupValidation dto = hopsworksClient.handleRequest(getRequest, FeatureGroupValidation.class);
    List<FeatureGroupValidation> validations;
    if (dto.getCount() == null) {
      validations = new ArrayList<>();
      validations.add(dto);
    } else {
      validations = dto.getItems();
    }
    LOGGER.info("Received validations: " + validations);
    return validations;
  }


  public FeatureGroupValidation put(FeatureGroupBase featureGroupBase,
      FeatureGroupValidation featureGroupValidation)
      throws FeatureStoreException, IOException {
    return put(featureGroupBase.getFeatureStore().getProjectId(), featureGroupBase.getFeatureStore().getId(),
            featureGroupBase.getId(), featureGroupValidation);
  }

  private FeatureGroupValidation put(Integer projectId, Integer featurestoreId, Integer entityId,
      FeatureGroupValidation featureGroupValidation)
      throws FeatureStoreException, IOException {

    HopsworksClient hopsworksClient = getInstance();
    String pathTemplate = PROJECT_PATH + FeatureStoreApi.FEATURE_STORE_PATH + RESULTS_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", projectId)
        .set("fsId", featurestoreId)
        .set("entityType", entityType.getValue())
        .set("entityId", entityId)
        .expand();

    FeatureGroupValidations validations =
        FeatureGroupValidations.builder().expectationResults(featureGroupValidation.getExpectationResults())
        .validationTime(featureGroupValidation.getValidationTime()).build();
    String results = hopsworksClient.getObjectMapper().writeValueAsString(validations);
    HttpPut putRequest = new HttpPut(uri);
    putRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    putRequest.setEntity(new StringEntity(results));

    LOGGER.info("Sending metadata request: " + uri);
    LOGGER.info(results);

    return hopsworksClient.handleRequest(putRequest, FeatureGroupValidation.class);
  }

}
