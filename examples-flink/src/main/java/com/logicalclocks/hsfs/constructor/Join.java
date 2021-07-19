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

import com.logicalclocks.hsfs.Feature;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

public class Join {

  @Getter
  @Setter
  private Query query;

  @Getter
  @Setter
  private List<Feature> on;
  @Getter
  @Setter
  private List<Feature> leftOn;
  @Getter
  @Setter
  private List<Feature> rightOn;

  @Getter
  @Setter
  private JoinType joinType;

  @Getter
  @Setter
  private String prefix;

  public Join(Query query, JoinType joinType, String prefix) {
    this.query = query;
    this.joinType = joinType;
    this.prefix = prefix;
  }

  public Join(Query query, List<Feature> on, JoinType joinType, String prefix) {
    this.query = query;
    this.on = on;
    this.joinType = joinType;
    this.prefix = prefix;
  }

  public Join(Query query, List<Feature> leftOn, List<Feature> rightOn, JoinType joinType, String prefix) {
    this.query = query;
    this.leftOn = leftOn;
    this.rightOn = rightOn;
    this.joinType = joinType;
    this.prefix = prefix;
  }
}
