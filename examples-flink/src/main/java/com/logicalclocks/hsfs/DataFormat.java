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

import com.fasterxml.jackson.annotation.JsonProperty;

public enum DataFormat {
  @JsonProperty("csv")
  CSV,
  @JsonProperty("tsv")
  TSV,
  @JsonProperty("parquet")
  PARQUET,
  @JsonProperty("avro")
  AVRO,
  @JsonProperty("image")
  IMAGE,
  @JsonProperty("orc")
  ORC,
  @JsonProperty("tfrecords")
  TFRECORDS,
  @JsonProperty("tfrecord")
  TFRECORD
}
