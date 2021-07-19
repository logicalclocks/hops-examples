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

package com.logicalclocks.hsfs.constructor;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.Storage;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@AllArgsConstructor
@NoArgsConstructor
public class FsQuery {
  @Getter
  @Setter
  private String query;

  @Getter
  @Setter
  private String queryOnline;

  @Getter
  @Setter
  private List<OnDemandFeatureGroupAlias> onDemandFeatureGroups;

  @Getter
  @Setter
  private List<HudiFeatureGroupAlias> hudiCachedFeatureGroups;

  public void removeNewLines() {
    query = query != null ? query.replace("\n", " ") : null;
    queryOnline = queryOnline != null ? queryOnline.replace("\n", " ") : null;
  }

  public String getStorageQuery(Storage storage) throws FeatureStoreException {
    switch (storage) {
      case OFFLINE:
        return query;
      case ONLINE:
        return queryOnline;
      default:
        throw new FeatureStoreException("Cannot run query on ALL storages");
    }
  }
}
