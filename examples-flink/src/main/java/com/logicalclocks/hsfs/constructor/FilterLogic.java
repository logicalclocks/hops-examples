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

import lombok.Getter;
import lombok.Setter;

public class FilterLogic {

  @Getter @Setter
  private SqlFilterLogic type;

  @Getter @Setter
  private Filter leftFilter;

  @Getter @Setter
  private Filter rightFilter;

  @Getter @Setter
  private FilterLogic leftLogic;

  @Getter @Setter
  private FilterLogic rightLogic;

  FilterLogic(Filter leftFilter) {
    this.type = SqlFilterLogic.SINGLE;
    this.leftFilter = leftFilter;
  }

  FilterLogic(SqlFilterLogic type, Filter leftFilter, Filter rightFilter) {
    this.type = type;
    this.leftFilter = leftFilter;
    this.rightFilter = rightFilter;
  }

  FilterLogic(SqlFilterLogic type, Filter leftFilter, FilterLogic rightLogic) {
    this.type = type;
    this.leftFilter = leftFilter;
    this.rightLogic = rightLogic;
  }

  FilterLogic(SqlFilterLogic type, FilterLogic leftLogic, Filter rightFilter) {
    this.type = type;
    this.leftLogic = leftLogic;
    this.rightFilter = rightFilter;
  }

  FilterLogic(SqlFilterLogic type, FilterLogic leftLogic, FilterLogic rightLogic) {
    this.type = type;
    this.leftLogic = leftLogic;
    this.rightLogic = rightLogic;
  }

  public FilterLogic and(Filter other) {
    return new FilterLogic(SqlFilterLogic.AND, this, other);
  }

  public FilterLogic and(FilterLogic other) {
    return new FilterLogic(SqlFilterLogic.AND, this, other);
  }

  public FilterLogic or(Filter other) {
    return new FilterLogic(SqlFilterLogic.OR, this, other);
  }

  public FilterLogic or(FilterLogic other) {
    return new FilterLogic(SqlFilterLogic.OR, this, other);
  }
}
