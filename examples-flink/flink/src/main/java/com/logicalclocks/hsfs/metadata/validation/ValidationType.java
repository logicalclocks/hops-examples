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

public enum ValidationType {
  // Data validation is performed and feature group is updated only if validation status is "Success"
  STRICT(1),
  // Data validation is performed and feature group is updated only if validation status is "Warning" or lower
  WARNING(2),
  // Data validation is performed and feature group is updated only if validation status is "Failure" or lower
  ALL(3),
  // Data validation not performed on feature group
  NONE(4);
  private final int severity;

  private ValidationType(int severity) {
    this.severity = severity;
  }

  public int getSeverity() {
    return severity;
  }

  public static ValidationType fromSeverity(int v) {
    for (ValidationType c : ValidationType.values()) {
      if (c.severity == v) {
        return c;
      }
    }
    throw new IllegalArgumentException("" + v);
  }
}
