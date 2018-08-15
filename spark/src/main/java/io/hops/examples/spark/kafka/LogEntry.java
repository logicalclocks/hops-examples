/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.hops.examples.spark.kafka;

import java.io.Serializable;

/**
 *
 * <p>
 */
public class LogEntry implements Serializable {

  private String message;
  private String priority;
  private String logger;
  private String timestamp;

  public LogEntry(String message, String priority, String logger, String timestamp) {
    this.message = message;
    this.priority = priority;
    this.logger = logger;
    this.timestamp = timestamp;
  }

  public String getMessage() {
    return message;
  }

  public String getPriority() {
    return priority;
  }

  public String getLogger() {
    return logger;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public void setPriority(String priority) {
    this.priority = priority;
  }

  public void setLogger(String logger) {
    this.logger = logger;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return "LogEntry{" + "message=" + message + ", priority=" + priority + ", logger=" + logger + ", timestamp="
        + timestamp + '}';
  }

}
