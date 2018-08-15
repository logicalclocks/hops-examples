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

package io.hops.examples.spark;

import java.io.Serializable;

/**
 *
 * <p>
 */
public class CSVMessage implements Serializable {

  private long positionMessageId;
  private long vehicleId;
  private String timeMessage;
  private String timeSave;
  private double lon;
  private double lat;
  private int speed;
  private int heading;
  private long fuelLevel;
  private long totalFuel;
  private int ignition;
  private int vehicleStopped;
  private int triggerType;
  private int dataSource;

  public CSVMessage(long positionMessageId, long vehicleId, String timeMessage, String timeSave, double lon,
      double lat, int speed, int heading, long fuelLevel, long totalFuel, int ignition, int vehicleStopped,
      int triggerType, int dataSource) {
    this.positionMessageId = positionMessageId;
    this.vehicleId = vehicleId;
    this.timeMessage = timeMessage;
    this.timeSave = timeSave;
    this.lon = lon;
    this.lat = lat;
    this.speed = speed;
    this.heading = heading;
    this.fuelLevel = fuelLevel;
    this.totalFuel = totalFuel;
    this.ignition = ignition;
    this.vehicleStopped = vehicleStopped;
    this.triggerType = triggerType;
    this.dataSource = dataSource;
  }

  public long getPositionMessageId() {
    return positionMessageId;
  }

  public void setPositionMessageId(long positionMessageId) {
    this.positionMessageId = positionMessageId;
  }

  public long getVehicleId() {
    return vehicleId;
  }

  public void setVehicleId(long vehicleId) {
    this.vehicleId = vehicleId;
  }

  public String getTimeMessage() {
    return timeMessage;
  }

  public void setTimeMessage(String timeMessage) {
    this.timeMessage = timeMessage;
  }

  public String getTimeSave() {
    return timeSave;
  }

  public void setTimeSave(String timeSave) {
    this.timeSave = timeSave;
  }

  public double getLon() {
    return lon;
  }

  public void setLon(double lon) {
    this.lon = lon;
  }

  public double getLat() {
    return lat;
  }

  public void setLat(double lat) {
    this.lat = lat;
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

  public long getFuelLevel() {
    return fuelLevel;
  }

  public void setFuelLevel(long fuelLevel) {
    this.fuelLevel = fuelLevel;
  }

  public long getTotalFuel() {
    return totalFuel;
  }

  public void setTotalFuel(long totalFuel) {
    this.totalFuel = totalFuel;
  }

  public int getIgnition() {
    return ignition;
  }

  public void setIgnition(int ignition) {
    this.ignition = ignition;
  }

  public int getVehicleStopped() {
    return vehicleStopped;
  }

  public void setVehicleStopped(int vehicleStopped) {
    this.vehicleStopped = vehicleStopped;
  }

  public int getTriggerType() {
    return triggerType;
  }

  public void setTriggerType(int triggerType) {
    this.triggerType = triggerType;
  }

  public int getDataSource() {
    return dataSource;
  }

  public void setDataSource(int dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public String toString() {
    return "CSVEntryUnified{" + "positionMessageId=" + positionMessageId + ", vehicleId=" + vehicleId + ", timeMessage="
        + timeMessage + ", timeSave=" + timeSave + ", lon=" + lon + ", lat=" + lat + ", speed=" + speed + ", heading="
        + heading + ", fuelLevel=" + fuelLevel + ", totalFuel=" + totalFuel + ", ignition=" + ignition
        + ", vehicleStopped=" + vehicleStopped + ", triggerType=" + triggerType + ", dataSource=" + dataSource + '}';
  }

}
