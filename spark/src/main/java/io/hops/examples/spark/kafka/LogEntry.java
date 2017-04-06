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
  private String thread;
  private String timestamp;

  public LogEntry(String message, String priority, String logger, String thread, String timestamp) {
    this.message = message;
    this.priority = priority;
    this.logger = logger;
    this.thread = thread;
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

  public String getThread() {
    return thread;
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

  public void setThread(String thread) {
    this.thread = thread;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return "LogEntry{" + "message=" + message + ", priority=" + priority + ", logger=" + logger + ", thread=" + thread
        + ", timestamp=" + timestamp + '}';
  }

}
