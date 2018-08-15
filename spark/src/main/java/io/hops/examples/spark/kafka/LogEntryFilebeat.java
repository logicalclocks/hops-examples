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
public class LogEntryFilebeat extends LogEntry implements Serializable {

  private String thread;
  private String file;

  public LogEntryFilebeat(String message, String priority, String logger, String thread, String timestamp, 
      String file) {
    super(message, priority, logger, timestamp);
    this.thread = thread;
    this.file = file;
  }

  public String getThread() {
    return thread;
  }

  public void setThread(String thread) {
    this.thread = thread;
  }

  public String getFile() {
    return file;
  }

  public void setFile(String file) {
    this.file = file;
  }

  @Override
  public String toString() {
    return "LogEntry{" + "message=" + super.getMessage() + ", priority=" + super.getPriority() + ", logger=" + super.
        getLogger() + ", thread=" + thread + ", timestamp=" + super.getTimestamp() + ", file=" + file + '}';
  }

}
