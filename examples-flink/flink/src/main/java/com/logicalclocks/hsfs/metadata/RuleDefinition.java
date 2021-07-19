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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.hsfs.metadata.validation.FeatureType;
import com.logicalclocks.hsfs.metadata.validation.RuleName;
import com.logicalclocks.hsfs.metadata.validation.Predicate;
import com.logicalclocks.hsfs.metadata.validation.AcceptedType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Builder
/*
  Used when fetching validation rules from the /rules resource.
 */
public class RuleDefinition extends RestDto<RuleDefinition> {

  @Getter @Setter
  private RuleName name;
  @Getter @Setter
  private Predicate predicate;
  @Getter @Setter
  private AcceptedType acceptedType;
  @Getter @Setter
  private FeatureType featureType;
  @Getter @Setter
  private String description;

}
