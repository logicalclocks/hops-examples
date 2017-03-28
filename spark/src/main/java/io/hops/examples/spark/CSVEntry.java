package io.hops.examples.spark;

import java.io.Serializable;

/**
 *
 * <p>
 */
public class CSVEntry implements Serializable {

  private int positionMessageId;
  private int vehicleId;
  private double lat;
  private double lon;
  private int speed;
  private int heading;
  private String timeMessage;
  private String timePosition;
  private String timeSave;

  public CSVEntry(int positionMessageId, int vehicleId, double lat, double lon, int speed, int heading,
      String timeMessage, String timePosition, String timeSave) {
    this.positionMessageId = positionMessageId;
    this.vehicleId = vehicleId;
    this.lat = lat;
    this.lon = lon;
    this.speed = speed;
    this.heading = heading;
    this.timeMessage = timeMessage;
    this.timePosition = timePosition;
    this.timeSave = timeSave;
  }

  public int getPositionMessageId() {
    return positionMessageId;
  }

  public void setPositionMessageId(int positionMessageId) {
    this.positionMessageId = positionMessageId;
  }

  public int getVehicleId() {
    return vehicleId;
  }

  public void setVehicleId(int vehicleId) {
    this.vehicleId = vehicleId;
  }

  public double getLat() {
    return lat;
  }

  public void setLat(double lat) {
    this.lat = lat;
  }

  public double getLon() {
    return lon;
  }

  public void setLon(double lon) {
    this.lon = lon;
  }

  public int getSpeed() {
    return speed;
  }

  public void setSpeed(int speed) {
    this.speed = speed;
  }

  public int getHeading() {
    return heading;
  }

  public void setHeading(int heading) {
    this.heading = heading;
  }

  public String getTimeMessage() {
    return timeMessage;
  }

  public void setTimeMessage(String timeMessage) {
    this.timeMessage = timeMessage;
  }

  public String getTimePosition() {
    return timePosition;
  }

  public void setTimePosition(String timePosition) {
    this.timePosition = timePosition;
  }

  public String getTimeSave() {
    return timeSave;
  }

  public void setTimeSave(String timeSave) {
    this.timeSave = timeSave;
  }

  @Override
  public String toString() {
    return "CSVEntry{" + "positionMessageId=" + positionMessageId + ", vehicleId=" + vehicleId + ", lat=" + lat
        + ", lon=" + lon + ", speed=" + speed + ", heading=" + heading + ", timeMessage=" + timeMessage
        + ", timePosition=" + timePosition + ", timeSave=" + timeSave + '}';
  }

}
