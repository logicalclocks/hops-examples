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

import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureGroupCommit;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.HopsworksHttpClient;
import com.logicalclocks.hsfs.metadata.KafkaApi;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FeatureGroupEngine {

  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  private HudiEngine hudiEngine = new HudiEngine();
  protected KafkaApi kafkaApi = new KafkaApi();

  private Utils utils = new Utils();

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroupEngine.class);

  private Map<Long, Map<String, String>>  getCommitDetails(FeatureGroup featureGroup, String wallclockTime,
                                                           Integer limit)
      throws FeatureStoreException, IOException, ParseException {

    Long wallclockTimestamp =  wallclockTime != null ? utils.getTimeStampFromDateString(wallclockTime) : null;
    List<FeatureGroupCommit> featureGroupCommits =
        featureGroupApi.getCommitDetails(featureGroup, wallclockTimestamp, limit);
    if (featureGroupCommits == null) {
      throw new FeatureStoreException("There are no commit details available for this Feature group");
    }
    Map<Long, Map<String, String>> commitDetails = new HashMap<>();
    for (FeatureGroupCommit featureGroupCommit : featureGroupCommits) {
      commitDetails.put(featureGroupCommit.getCommitID(), new HashMap<String, String>() {{
            put("committedOn", hudiEngine.timeStampToHudiFormat(featureGroupCommit.getCommitID()));
            put("rowsUpdated", featureGroupCommit.getRowsUpdated() != null
                ? featureGroupCommit.getRowsUpdated().toString() : "0");
            put("rowsInserted", featureGroupCommit.getRowsInserted() != null
                ? featureGroupCommit.getRowsInserted().toString() : "0");
            put("rowsDeleted", featureGroupCommit.getRowsDeleted() != null
                ? featureGroupCommit.getRowsDeleted().toString() : "0");
          }}
      );
    }
    return commitDetails;
  }

  public Map<Long, Map<String, String>> commitDetails(FeatureGroup featureGroup, Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    // operation is only valid for time travel enabled feature group
    if (featureGroup.getTimeTravelFormat() == TimeTravelFormat.NONE) {
      throw new FeatureStoreException("commitDetails function is only valid for "
          + "time travel enabled feature group");
    }
    return getCommitDetails(featureGroup, null, limit);
  }

  public Map<Long, Map<String, String>> commitDetailsByWallclockTime(FeatureGroup featureGroup,
                                                                     String wallclockTime, Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    return getCommitDetails(featureGroup, wallclockTime, limit);
  }

  public String getAvroSchema(FeatureGroup featureGroup) throws FeatureStoreException, IOException {
    return kafkaApi.getTopicSubject(featureGroup.getFeatureStore(), featureGroup.getOnlineTopicName()).getSchema();
  }

  // TODO(Fabio): why is this here?
  public Map<String, String> getKafkaConfig(FeatureGroup featureGroup, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    Map<String, String> config = new HashMap<>();
    if (writeOptions != null) {
      config.putAll(writeOptions);
    }
    HopsworksHttpClient client = HopsworksClient.getInstance().getHopsworksHttpClient();

    config.put("kafka.bootstrap.servers",
        kafkaApi.getBrokerEndpoints(featureGroup.getFeatureStore()).stream().map(broker -> broker.replaceAll(
            "INTERNAL://", "")).collect(Collectors.joining(",")));
    config.put("kafka.security.protocol", "SSL");
    config.put("kafka.ssl.truststore.location", client.getTrustStorePath());
    config.put("kafka.ssl.truststore.password", client.getCertKey());
    config.put("kafka.ssl.keystore.location", client.getKeyStorePath());
    config.put("kafka.ssl.keystore.password", client.getCertKey());
    config.put("kafka.ssl.key.password", client.getCertKey());
    config.put("kafka.ssl.endpoint.identification.algorithm", "");
    return config;
  }
}
