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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Builder
public class ExpectationResult {

  @Getter @Setter
  private Status status; //Set by backend
  @Getter @Setter
  private Expectation expectation;
  @Getter @Setter
  private List<ValidationResult> results;

  public enum Status {
    NONE("None",0),
    SUCCESS("Success",1),
    WARNING("Warning",2),
    FAILURE("Failure",3);

    private final String name;
    private final int severity;

    Status(String name, int severity) {
      this.name = name;
      this.severity = severity;
    }

    public int getSeverity() {
      return severity;
    }

    public static Status fromString(String name) {
      return valueOf(name.toUpperCase());
    }

    public String getName() {
      return name;
    }

    @Override
    public String toString() {
      return name;
    }
  }
}
