package io.hops.examples.spark.kafka;

import java.io.Serializable;

/**
 *
 * @author tkak
 */
public class LogEntry implements Serializable {

  private String message;
  private String priority;

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public String getPriority() {
    return priority;
  }

  public void setPriority(String priority) {
    this.priority = priority;
  }

  @Override
  public String toString() {
    return "LogEntry{" + "message=" + message + ", priority=" + priority + '}';
  }

}
