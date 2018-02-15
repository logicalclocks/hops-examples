package io.hops.examples.spark.kafka;

import java.io.Serializable;

/**
 *
 * <p>
 */
public class IntrabodyMessage implements Serializable {

  private String recordUUID;
  private long epochMillis;
  private String deviceUUID;
  private long bootNum;
  private long bootMillis;
  private double longitude;
  private double latitude;

  public IntrabodyMessage(String recordUUID, String epochMillis, String deviceUUID, String bootNum, String bootMillis,
      String longitude, String latitude) {
    this.recordUUID = recordUUID;
    this.epochMillis = Long.parseLong(epochMillis);
    this.deviceUUID = deviceUUID;
    this.bootNum = Long.parseLong(bootNum);
    this.bootMillis = Long.parseLong(bootMillis);
    this.longitude = Double.parseDouble(longitude);
    this.latitude = Double.parseDouble(latitude);
  }

  public String getRecordUUID() {
    return recordUUID;
  }

  public void setRecordUUID(String recordUUID) {
    this.recordUUID = recordUUID;
  }

  public long getEpochMillis() {
    return epochMillis;
  }

  public void setEpochMillis(long epochMillis) {
    this.epochMillis = epochMillis;
  }

  public String getDeviceUUID() {
    return deviceUUID;
  }

  public void setDeviceUUID(String deviceUUID) {
    this.deviceUUID = deviceUUID;
  }

  public long getBootNum() {
    return bootNum;
  }

  public void setBootNum(long bootNum) {
    this.bootNum = bootNum;
  }

  public long getBootMillis() {
    return bootMillis;
  }

  public void setBootMillis(long bootMillis) {
    this.bootMillis = bootMillis;
  }

  public double getLongitude() {
    return longitude;
  }

  public void setLongitude(double longitude) {
    this.longitude = longitude;
  }

  public double getLatitude() {
    return latitude;
  }

  public void setLatitude(double latitude) {
    this.latitude = latitude;
  }

  @Override
  public String toString() {
    return "IntrabodyMessage{" + "recordUUID=" + recordUUID + ", epochMillis=" + epochMillis + ", deviceUUID="
        + deviceUUID + ", bootNum=" + bootNum + ", bootMillis=" + bootMillis + ", longitude=" + longitude
        + ", latitude=" + latitude + '}';
  }

}
