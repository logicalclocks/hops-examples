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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.hsfs.metadata.RestDto;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Setter;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FeatureGroupCommit extends RestDto<FeatureGroupCommit> {
  @Getter
  @Setter
  private Long commitID;
  @Getter
  @Setter
  private String commitDateString;
  @Getter
  @Setter
  private Long commitTime;
  @Getter
  @Setter
  private Long rowsInserted;
  @Getter
  @Setter
  private Long rowsUpdated;
  @Getter
  @Setter
  private Long rowsDeleted;
  @Getter
  @Setter
  private Integer validationId;
}
