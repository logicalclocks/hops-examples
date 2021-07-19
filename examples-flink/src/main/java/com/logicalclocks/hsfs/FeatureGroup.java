/*
 * Copyright (c) 2020 Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.hsfs.engine.FeatureGroupEngine;
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;
import com.logicalclocks.hsfs.engine.StatisticsEngine;
import com.logicalclocks.hsfs.metadata.Expectation;
import com.logicalclocks.hsfs.metadata.validation.ValidationType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.avro.Schema;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FeatureGroup extends FeatureGroupBase {

  @Getter
  @Setter
  private Boolean onlineEnabled;

  @Getter
  @Setter
  private String type = "cachedFeaturegroupDTO";

  @Getter
  @Setter
  private TimeTravelFormat timeTravelFormat = TimeTravelFormat.HUDI;

  @Getter
  @Setter
  protected String location;

  @Getter
  @Setter
  private List<String> statisticColumns;

  @JsonIgnore
  // These are only used in the client. In the server they are aggregated in the `features` field
  private List<String> primaryKeys;

  @JsonIgnore
  // These are only used in the client. In the server they are aggregated in the `features` field
  private List<String> partitionKeys;

  @JsonIgnore
  // This is only used in the client. In the server they are aggregated in the `features` field
  private String hudiPrecombineKey;

  @JsonIgnore
  private String avroSchema;

  @Getter
  @Setter
  private String onlineTopicName;

  private final FeatureGroupEngine featureGroupEngine = new FeatureGroupEngine();
  private final StatisticsEngine statisticsEngine = new StatisticsEngine(EntityEndpointType.FEATURE_GROUP);

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroup.class);

  @Builder
  public FeatureGroup(FeatureStore featureStore, @NonNull String name, Integer version, String description,
                      List<String> primaryKeys, List<String> partitionKeys, String hudiPrecombineKey,
                      boolean onlineEnabled, TimeTravelFormat timeTravelFormat, List<Feature> features,
                      StatisticsConfig statisticsConfig,  ValidationType validationType,
                      scala.collection.Seq<Expectation> expectations, String onlineTopicName) {
    this.featureStore = featureStore;
    this.name = name;
    this.version = version;
    this.description = description;
    this.primaryKeys = primaryKeys != null
        ? primaryKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
    this.partitionKeys = partitionKeys != null
        ? partitionKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
    this.hudiPrecombineKey = timeTravelFormat == TimeTravelFormat.HUDI && hudiPrecombineKey != null
        ? hudiPrecombineKey.toLowerCase() : null;
    this.onlineEnabled = onlineEnabled;
    this.timeTravelFormat = timeTravelFormat != null ? timeTravelFormat : TimeTravelFormat.HUDI;
    this.features = features;
    this.statisticsConfig = statisticsConfig != null ? statisticsConfig : new StatisticsConfig();
    this.validationType = validationType != null ? validationType : ValidationType.NONE;
    if (expectations != null && !expectations.isEmpty()) {
      this.expectationsNames = new ArrayList<>();
      this.expectations = JavaConverters.seqAsJavaListConverter(expectations).asJava();
      this.expectations.forEach(expectation -> this.expectationsNames.add(expectation.getName()));
    }
    this.onlineTopicName = onlineTopicName;
  }

  public FeatureGroup() {
  }

  /**
   * Return commit details.
   *
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Map<Long, Map<String, String>> commitDetails() throws IOException, FeatureStoreException, ParseException {
    return featureGroupEngine.commitDetails(this, null);
  }

  /**
   * Return commit details.
   *
   * @param limit number of commits to return.
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Map<Long, Map<String, String>> commitDetails(Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    return featureGroupEngine.commitDetails(this, limit);
  }

  /**
   * Return commit details.
   *
   * @param wallclockTime point in time.
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Map<Long, Map<String, String>> commitDetails(String wallclockTime)
      throws IOException, FeatureStoreException, ParseException {
    return featureGroupEngine.commitDetailsByWallclockTime(this, wallclockTime, null);
  }

  /**
   * Return commit details.
   *
   * @param wallclockTime point in time.
   * @param limit number of commits to return.
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Map<Long, Map<String, String>> commitDetails(String wallclockTime, Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    return featureGroupEngine.commitDetailsByWallclockTime(this, wallclockTime, limit);
  }

  @JsonIgnore
  public String getAvroSchema() throws FeatureStoreException, IOException {
    if (avroSchema == null) {
      avroSchema = featureGroupEngine.getAvroSchema(this);
    }
    return avroSchema;
  }

  @JsonIgnore
  public List<String> getComplexFeatures() {
    return features.stream().filter(Feature::isComplex).map(Feature::getName).collect(Collectors.toList());
  }

  @JsonIgnore
  public String getFeatureAvroSchema(String featureName) throws FeatureStoreException, IOException {
    Schema schema = getDeserializedAvroSchema();
    Schema.Field complexField = schema.getFields().stream().filter(field ->
        field.name().equalsIgnoreCase(featureName)).findFirst().orElseThrow(() ->
            new FeatureStoreException(
                "Complex feature `" + featureName + "` not found in AVRO schema of online feature group."));

    return complexField.schema().toString(true);
  }

  @JsonIgnore
  public String getEncodedAvroSchema() throws FeatureStoreException, IOException {
    Schema schema = getDeserializedAvroSchema();
    List<Schema.Field> fields = schema.getFields().stream()
        .map(field -> getComplexFeatures().contains(field.name())
            ? new Schema.Field(field.name(), SchemaBuilder.builder().nullable().bytesType(), null, null)
            : new Schema.Field(field.name(), field.schema(), null, null))
        .collect(Collectors.toList());
    return Schema.createRecord(schema.getName(), null, schema.getNamespace(), schema.isError(), fields).toString(true);
  }

  @JsonIgnore
  public Schema getDeserializedAvroSchema() throws FeatureStoreException, IOException {
    try {
      return new Schema.Parser().parse(getAvroSchema());
    } catch (SchemaParseException e) {
      throw new FeatureStoreException("Failed to deserialize online feature group schema" + getAvroSchema() + ".");
    }
  }

  @JsonIgnore
  public List<String> getPrimaryKeys() {
    if (primaryKeys == null) {
      primaryKeys = features.stream().filter(f -> f.getPrimary()).map(Feature::getName).collect(Collectors.toList());
    }
    return primaryKeys;
  }
}
