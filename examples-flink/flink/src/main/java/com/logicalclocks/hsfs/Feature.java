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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Strings;
import com.logicalclocks.hsfs.constructor.Filter;
import com.logicalclocks.hsfs.constructor.SqlFilterCondition;
import com.logicalclocks.hsfs.util.Constants;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
public class Feature {
  @Getter
  private String name;

  @Getter
  @Setter
  private String type;

  @Getter
  @Setter
  private String onlineType;

  @Getter
  @Setter
  private String description;

  @Getter
  @Setter
  private Boolean primary;

  @Getter
  @Setter
  private Boolean partition;

  @Getter
  @Setter
  private Boolean hudiPrecombineKey = false;

  @Getter
  @Setter
  private String defaultValue;

  @Getter
  @Setter
  private Integer featureGroupId;

  public Feature(@NonNull String name) {
    setName(name);
  }

  public Feature(@NonNull String name, @NonNull FeatureGroup featureGroup) {
    setName(name);
    this.featureGroupId = featureGroup.getId();
  }

  public Feature(@NonNull String name, @NonNull String type) {
    this.name = name.toLowerCase();
    this.type = type;
  }

  public Feature(@NonNull String name, @NonNull String type, @NonNull String defaultValue) {
    setName(name);
    this.type = type;
    this.defaultValue = defaultValue;
  }

  public Feature(String name, String type, Boolean primary, Boolean partition)
      throws FeatureStoreException {
    if (Strings.isNullOrEmpty(name)) {
      throw new FeatureStoreException("Name is required when creating a feature");
    }
    setName(name);

    if (Strings.isNullOrEmpty(type)) {
      throw new FeatureStoreException("Type is required when creating a feature");
    }
    this.type = type;
    this.primary = primary;
    this.partition = partition;
  }

  @Builder
  public Feature(String name, String type, String onlineType, Boolean primary, Boolean partition, String defaultValue)
      throws FeatureStoreException {
    if (Strings.isNullOrEmpty(name)) {
      throw new FeatureStoreException("Name is required when creating a feature");
    }
    setName(name);

    if (Strings.isNullOrEmpty(type)) {
      throw new FeatureStoreException("Type is required when creating a feature");
    }
    this.type = type;
    this.onlineType = onlineType;
    this.primary = primary;
    this.partition = partition;
    this.defaultValue = defaultValue;
  }

  @JsonIgnore
  public boolean isComplex() {
    return Constants.COMPLEX_FEATURE_TYPES.stream().anyMatch(c -> type.toUpperCase().startsWith(c));
  }

  public void setName(String name) {
    this.name = name.toLowerCase();
  }

  public Filter lt(Object value) {
    return new Filter(this, SqlFilterCondition.LESS_THAN, value.toString());
  }

  public Filter le(Object value) {
    return new Filter(this, SqlFilterCondition.LESS_THAN_OR_EQUAL, value.toString());
  }

  public Filter eq(Object value) {
    return new Filter(this, SqlFilterCondition.EQUALS, value.toString());
  }

  public Filter ne(Object value) {
    return new Filter(this, SqlFilterCondition.NOT_EQUALS, value.toString());
  }

  public Filter gt(Object value) {
    return new Filter(this, SqlFilterCondition.GREATER_THAN, value.toString());
  }

  public Filter ge(Object value) {
    return new Filter(this, SqlFilterCondition.GREATER_THAN_OR_EQUAL, value.toString());
  }
}
