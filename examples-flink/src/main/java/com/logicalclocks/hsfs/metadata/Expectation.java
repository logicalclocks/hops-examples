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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.engine.ExpectationsEngine;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.List;
import com.logicalclocks.hsfs.metadata.validation.Rule;

@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Builder
public class Expectation extends RestDto<Expectation> {

  @Getter @Setter
  private String name;
  @Getter @Setter
  private String description;
  @Getter @Setter
  private List<String> features;
  @Getter @Setter
  private List<Rule> rules;
  @Getter @Setter
  @JsonIgnore
  private FeatureStore featureStore;

  private final ExpectationsEngine expectationsEngine = new ExpectationsEngine();

  public static class ExpectationBuilder {

    public ExpectationBuilder features(List<String> features) {
      this.features = features;
      return this;
    }

    public ExpectationBuilder features(scala.collection.Seq<String> features) {
      this.features = (List<String>) JavaConverters.seqAsJavaListConverter(features).asJava();
      return this;
    }

    public ExpectationBuilder rules(scala.collection.Seq<Rule> rules) {
      this.rules = (List<Rule>) JavaConverters.seqAsJavaListConverter(rules).asJava();
      return this;
    }

    public ExpectationBuilder rules(List<Rule> rules) {
      this.rules = rules;
      return this;
    }
  }

  public void save() throws FeatureStoreException, IOException {
    expectationsEngine.save(this);
  }
}
