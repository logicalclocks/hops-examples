/*
 * Copyright (c) 2020 Logical Clocks AB *
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

import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;
import com.logicalclocks.hsfs.metadata.TagsApi;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FeatureGroupBaseEngine {
  protected FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  protected TagsApi tagsApi = new TagsApi(EntityEndpointType.FEATURE_GROUP);

  public void delete(FeatureGroupBase featureGroupBase) throws FeatureStoreException, IOException {
    featureGroupApi.delete(featureGroupBase);
  }

  public void addTag(FeatureGroupBase featureGroupBase, String name, Object value)
      throws FeatureStoreException, IOException {
    tagsApi.add(featureGroupBase, name, value);
  }

  public Object getTag(FeatureGroupBase featureGroupBase, String name) throws FeatureStoreException, IOException {
    return tagsApi.get(featureGroupBase, name);
  }

  public Map<String, Object> getTags(FeatureGroupBase featureGroupBase) throws FeatureStoreException, IOException {
    return tagsApi.get(featureGroupBase);
  }

  public void deleteTag(FeatureGroupBase featureGroupBase, String name)
      throws FeatureStoreException, IOException {
    tagsApi.deleteTag(featureGroupBase, name);
  }

  public void updateDescription(FeatureGroupBase featureGroup, String description)
      throws FeatureStoreException, IOException {
    FeatureGroupBase fgBaseSend = new FeatureGroupBase(featureGroup.getFeatureStore(), featureGroup.getId());
    fgBaseSend.setDescription(description);
    FeatureGroup apiFG = featureGroupApi.updateMetadata(fgBaseSend, "updateMetadata");
    featureGroup.setDescription(apiFG.getDescription());
  }

  public void appendFeatures(FeatureGroupBase featureGroup, List<Feature> features)
      throws FeatureStoreException, IOException {
    FeatureGroupBase fgBaseSend = new FeatureGroupBase(featureGroup.getFeatureStore(), featureGroup.getId());
    features.addAll(featureGroup.getFeatures());
    fgBaseSend.setFeatures(features);
    FeatureGroup apiFG = featureGroupApi.updateMetadata(fgBaseSend, "updateMetadata");
    featureGroup.setFeatures(apiFG.getFeatures());
  }

  public void updateStatisticsConfig(FeatureGroup featureGroup) throws FeatureStoreException, IOException {
    FeatureGroup apiFG = featureGroupApi.updateMetadata(featureGroup, "updateStatsConfig");
    featureGroup.getStatisticsConfig().setCorrelations(apiFG.getStatisticsConfig().getCorrelations());
    featureGroup.getStatisticsConfig().setHistograms(apiFG.getStatisticsConfig().getHistograms());
  }

  public void updateValidationType(FeatureGroupBase featureGroupBase) throws FeatureStoreException, IOException {
    featureGroupApi.updateMetadata(featureGroupBase, "validationType", featureGroupBase.getValidationType());
  }
}
