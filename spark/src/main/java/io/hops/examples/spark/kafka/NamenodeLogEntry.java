package io.hops.examples.spark.kafka;

import java.io.Serializable;

/**
 *
 * <p>
 */
public class NamenodeLogEntry implements Serializable {

  private String message;
  private String priority;
  private String logger;
  private String timestamp;
  private String file;

  public NamenodeLogEntry(String message, String priority, String logger, String timestamp, String file) {
    this.message = message;
    this.priority = priority;
    this.logger = logger;
    this.timestamp = timestamp;
    this.file = file;
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

  public String getFile() {
    return file;
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

  public void setFile(String file) {
    this.file = file;
  }

}
