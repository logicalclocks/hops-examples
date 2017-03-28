package io.hops.examples.spark.kafka;

import java.io.Serializable;

/**
 *
 * @author tkak
 */
public class LogLine implements Serializable {

  private String line;

  public String getLine() {
    return line;
  }

  public void setLine(String line) {
    this.line = line;
  }

  @Override
  public String toString() {
    return "LogLine{" + "line=" + line + '}';
  }

}
