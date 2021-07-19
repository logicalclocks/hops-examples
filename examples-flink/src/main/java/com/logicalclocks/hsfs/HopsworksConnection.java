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

import com.google.common.base.Strings;
import com.logicalclocks.hsfs.metadata.FeatureStoreApi;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.ProjectApi;
import com.logicalclocks.hsfs.metadata.RuleDefinition;
import com.logicalclocks.hsfs.metadata.RulesApi;
import com.logicalclocks.hsfs.metadata.validation.RuleName;
import com.logicalclocks.hsfs.util.Constants;
import lombok.Builder;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;

import java.io.Closeable;
import java.io.IOException;

public class HopsworksConnection implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(HopsworksConnection.class);

  @Getter
  private String host;

  @Getter
  private int port;

  @Getter
  private String project;

  @Getter
  private Region region;

  @Getter
  private SecretStore secretStore;

  @Getter
  private boolean hostnameVerification;

  @Getter
  private String trustStorePath;

  @Getter
  private String certPath;

  @Getter
  private String apiKeyFilePath;

  @Getter
  private String apiKeyValue;

  private FeatureStoreApi featureStoreApi = new FeatureStoreApi();
  private ProjectApi projectApi = new ProjectApi();
  private RulesApi rulesApi = new RulesApi();

  private Project projectObj;

  @Builder
  public HopsworksConnection(String host, int port, String project, Region region, SecretStore secretStore,
                             boolean hostnameVerification, String trustStorePath,
                             String certPath, String apiKeyFilePath, String apiKeyValue)
      throws IOException, FeatureStoreException {
    this.host = host;
    this.port = port;
    this.project = getProjectName(project);
    this.region = region;
    this.secretStore = secretStore;
    this.hostnameVerification = hostnameVerification;
    this.trustStorePath = trustStorePath;
    this.certPath = certPath;
    this.apiKeyFilePath = apiKeyFilePath;
    this.apiKeyValue = apiKeyValue;

    HopsworksClient.setupHopsworksClient(host, port, region, secretStore,
        hostnameVerification, trustStorePath, this.apiKeyFilePath, this.apiKeyValue);
    this.projectObj = getProject();
    HopsworksClient.getInstance().setProject(this.projectObj);
  }

  /**
   * Retrieve the project feature store.
   *
   * @return FeatureStore
   * @throws IOException
   * @throws FeatureStoreException
   */
  public FeatureStore getFeatureStore() throws IOException, FeatureStoreException {
    return getFeatureStore(project.toLowerCase() + Constants.FEATURESTORE_SUFFIX);
  }

  /**
   * Retrieve a feature store based on name. The feature store needs to be shared with
   * the connection's project.
   *
   * @param name the name of the feature store to get the handle for
   * @return FeatureStore
   * @throws IOException
   * @throws FeatureStoreException
   */
  public FeatureStore getFeatureStore(String name) throws IOException, FeatureStoreException {
    return featureStoreApi.get(projectObj.getProjectId(), name);
  }

  /**
   * Close the connection and clean up the certificates.
   */
  public void close() {
    // Close the client
  }

  private Project getProject() throws IOException, FeatureStoreException {
    LOGGER.info("Getting information for project name: " + project);
    return projectApi.get(project);
  }

  private String getProjectName(String project) {
    if (Strings.isNullOrEmpty(project)) {
      // User didn't specify a project in the connection construction. Assume they are running
      // from within Hopsworks and the project name is available a system property
      return System.getProperty(Constants.PROJECTNAME_ENV);
    }
    return project;
  }

  public scala.collection.Seq<RuleDefinition> getRules() throws FeatureStoreException, IOException {
    return rulesApi.get();
  }

  public RuleDefinition getRule(RuleName name) throws FeatureStoreException, IOException {
    return rulesApi.get(name);
  }
}
