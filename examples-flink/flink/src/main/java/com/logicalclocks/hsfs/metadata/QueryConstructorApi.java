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
import com.logicalclocks.hsfs.constructor.Query;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class QueryConstructorApi {

  public static final String QUERY_CONSTRUCTOR_PATH = "/query";

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryConstructorApi.class);

  public FsQuery constructQuery(FeatureStore featureStore, Query query) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_SERVICE_PATH
        + QUERY_CONSTRUCTOR_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureStore.getProjectId())
        .expand();

    String queryJson = hopsworksClient.getObjectMapper().writeValueAsString(query);
    HttpPut putRequest = new HttpPut(uri);
    putRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    putRequest.setEntity(new StringEntity(queryJson));

    LOGGER.info("Sending metadata request: " + uri);
    LOGGER.info("Sending query: " + queryJson);
    FsQuery fsQuery = hopsworksClient.handleRequest(putRequest, FsQuery.class);
    fsQuery.removeNewLines();
    return fsQuery;
  }
}
