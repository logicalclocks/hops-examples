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

package com.logicalclocks.hsfs.metadata.validation;

public enum RuleName {
  HAS_MEAN,
  HAS_MIN,
  HAS_MAX,
  HAS_SUM,
  HAS_SIZE,
  HAS_COMPLETENESS,
  HAS_UNIQUENESS,
  HAS_DISTINCTNESS,
  HAS_UNIQUE_VALUE_RATIO,
  HAS_NUMBER_OF_DISTINCT_VALUES,
  HAS_ENTROPY,
  HAS_MUTUAL_INFORMATION,
  HAS_APPROX_QUANTILE,
  HAS_STANDARD_DEVIATION,
  HAS_APPROX_COUNT_DISTINCT,
  HAS_CORRELATION,
  HAS_PATTERN,
  HAS_MIN_LENGTH,
  HAS_MAX_LENGTH,
  HAS_DATATYPE,
  IS_NON_NEGATIVE,
  IS_POSITIVE,
  IS_LESS_THAN,
  IS_LESS_THAN_OR_EQUAL_TO,
  IS_GREATER_THAN,
  IS_GREATER_THAN_OR_EQUAL_TO,
  IS_CONTAINED_IN
}
