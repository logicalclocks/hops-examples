package io.hops.examples.spark.kafka;

import java.io.Serializable;

/**
 *
 * <p>
 */
public class LogEntryFilebeat extends LogEntry implements Serializable {

  private String thread;
  private String file;

  public LogEntryFilebeat(String message, String priority, String logger, String thread, String timestamp, String file) {
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
