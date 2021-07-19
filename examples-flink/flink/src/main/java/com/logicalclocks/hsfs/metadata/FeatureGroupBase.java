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

package com.logicalclocks.hsfs.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.constructor.Filter;
import com.logicalclocks.hsfs.constructor.FilterLogic;
import com.logicalclocks.hsfs.constructor.Query;
import com.logicalclocks.hsfs.engine.DataValidationEngine;
import com.logicalclocks.hsfs.engine.FeatureGroupBaseEngine;
import com.logicalclocks.hsfs.engine.StatisticsEngine;
import com.logicalclocks.hsfs.metadata.validation.ValidationType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.tuple.ImmutablePair;
/*
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
*/
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@NoArgsConstructor
public class FeatureGroupBase {

  @Getter
  @Setter
  protected Integer id;

  @Getter
  @Setter
  protected FeatureStore featureStore;

  @Getter
  @Setter
  protected String name;

  @Getter
  @Setter
  protected Integer version;

  @Getter
  @Setter
  protected String description;

  @Getter
  @Setter
  protected List<Feature> features;

  @Getter
  protected Date created;

  @Getter
  protected String creator;

  @Getter
  @Setter
  protected StatisticsConfig statisticsConfig = new StatisticsConfig();

  @Getter
  @Setter
  protected ValidationType validationType = ValidationType.NONE;

  @Getter
  @Setter
  protected List<String> expectationsNames;

  @JsonIgnore
  protected List<Expectation> expectations;

  private FeatureGroupBaseEngine featureGroupBaseEngine = new FeatureGroupBaseEngine();
  protected StatisticsEngine statisticsEngine = new StatisticsEngine(EntityEndpointType.FEATURE_GROUP);
  protected final ExpectationsApi expectationsApi = new ExpectationsApi(EntityEndpointType.FEATURE_GROUP);

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroupBase.class);

  public FeatureGroupBase(FeatureStore featureStore, Integer id) {
    this.featureStore = featureStore;
    this.id = id;
  }

  public Query selectFeatures(List<Feature> features) {
    return new Query(this, features);
  }

  public Query select(List<String> features) {
    // Create a feature object for each string feature given by the user.
    // For the query building each feature need only the name set.
    List<Feature> featureObjList = features.stream().map(Feature::new).collect(Collectors.toList());
    return selectFeatures(featureObjList);
  }

  public Query selectAll() {
    return new Query(this, getFeatures());
  }

  public Query selectExceptFeatures(List<Feature> features) {
    List<String> exceptFeatures = features.stream().map(Feature::getName).collect(Collectors.toList());
    return selectExcept(exceptFeatures);
  }

  public Query selectExcept(List<String> features) {
    return new Query(this,
        getFeatures().stream().filter(f -> !features.contains(f.getName())).collect(Collectors.toList()));
  }

  public void delete() throws FeatureStoreException, IOException {
    featureGroupBaseEngine.delete(this);
  }

  /*
  public Dataset<Row> read() throws FeatureStoreException, IOException {
    // This method should be overridden by the FeatureGroup/OnDeamandFeatureGroup classes
    return null;
  }
  */

  /**
   * Add name/value tag to the feature group.
   *
   * @param name  name of the tag
   * @param value value of the tag. The value of a tag can be any valid json - primitives, arrays or json objects.
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void addTag(String name, Object value) throws FeatureStoreException, IOException {
    featureGroupBaseEngine.addTag(this, name, value);
  }

  /**
   * Get all tags of the feature group.
   *
   * @return map of tag name and values. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public Map<String, Object> getTags() throws FeatureStoreException, IOException {
    return featureGroupBaseEngine.getTags(this);
  }

  /**
   * Get a single tag value of the feature group.
   *
   * @param name name of tha tag
   * @return The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public Object getTag(String name) throws FeatureStoreException, IOException {
    return featureGroupBaseEngine.getTag(this, name);
  }

  /**
   * Delete a tag of the feature group.
   *
   * @param name name of the tag to be deleted
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void deleteTag(String name) throws FeatureStoreException, IOException {
    featureGroupBaseEngine.deleteTag(this, name);
  }

  /**
   * Update the description of the feature group.
   *
   * @param description
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void updateDescription(String description) throws FeatureStoreException, IOException {
    featureGroupBaseEngine.updateDescription(this, description);
  }

  /**
   * Append features to the schema of the feature group.
   * It is only possible to append features to a feature group. Removing features is considered a breaking change.
   *
   * @param features
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void appendFeatures(List<Feature> features) throws FeatureStoreException, IOException {
    featureGroupBaseEngine.appendFeatures(this, new ArrayList<>(features));
  }

  /**
   * Append a single feature to the schema of the feature group.
   * It is only possible to append features to a feature group. Removing features is considered a breaking change.
   *
   * @param features
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void appendFeatures(Feature features) throws FeatureStoreException, IOException {
    List<Feature> featureList = new ArrayList<>();
    featureList.add(features);
    featureGroupBaseEngine.appendFeatures(this, featureList);
  }

  /**
   * Update the statistics configuration of the feature group.
   * Change the `enabled`, `histograms`, `correlations` or `columns` attributes and persist
   * the changes by calling this method.
   *
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void updateStatisticsConfig() throws FeatureStoreException, IOException {
    featureGroupBaseEngine.updateStatisticsConfig((FeatureGroup) this);
  }

  /**
   * Recompute the statistics for the feature group and save them to the feature store.
   *
   * @return statistics object of computed statistics
   * @throws FeatureStoreException
   * @throws IOException
   */
  /*
  public Statistics computeStatistics() throws FeatureStoreException, IOException {
    if (statisticsConfig.getEnabled()) {
      return statisticsEngine.computeStatistics(this, read(), null);
    } else {
      LOGGER.info("StorageWarning: The statistics are not enabled of feature group `" + name + "`, with version `"
          + version + "`. No statistics computed.");
    }
    return null;
  }
  */

  /**
   * Get the last statistics commit for the feature group.
   *
   * @return statistics object of latest commit
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public Statistics getStatistics() throws FeatureStoreException, IOException {
    return statisticsEngine.getLast(this);
  }

  /**
   * Get the statistics of a specific commit time for the feature group.
   *
   * @param commitTime commit time in the format "YYYYMMDDhhmmss"
   * @return statistics object for the commit time
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public Statistics getStatistics(String commitTime) throws FeatureStoreException, IOException {
    return statisticsEngine.get(this, commitTime);
  }

  /**
   * Filter the query based on a condition for a feature or a conjunction of multiple filters.
   *
   * @param filter
   * @return Query
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Query filter(Filter filter) throws FeatureStoreException, IOException {
    return this.selectAll().filter(filter);
  }

  /**
   * Filter the query based on a condition for a feature or a conjunction of multiple filters.
   *
   * @param filter
   * @return Query
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Query filter(FilterLogic filter) throws FeatureStoreException, IOException {
    return this.selectAll().filter(filter);
  }

  /**
   * Retrieve a feature of the feature group by name.
   *
   * @param name
   * @return Feature
   * @throws FeatureStoreException
   */
  @JsonIgnore
  public Feature getFeature(String name) throws FeatureStoreException {
    return features.stream().filter(f -> f.getName().equalsIgnoreCase(name))
        .findFirst()
        .orElseThrow(() -> new FeatureStoreException("Feature with name `" + name
            + "` not found in feature group `" + this.name + "`."));
  }

  public Expectation getExpectation(String name) throws FeatureStoreException, IOException {
    return expectationsApi.get(this, name);
  }

  @JsonIgnore
  public scala.collection.Seq<Expectation> getExpectations() throws FeatureStoreException, IOException {
    return JavaConverters.asScalaBufferConverter(expectationsApi.get(this)).asScala().toSeq();
  }

  public scala.collection.Seq<Expectation> attachExpectations(scala.collection.Seq<Expectation> expectations)
          throws FeatureStoreException, IOException {
    List<Expectation> expectationsList = new ArrayList<>();
    for (Expectation expectation : (List<Expectation>) JavaConverters.seqAsJavaListConverter(expectations).asJava()) {
      expectationsList.add(attachExpectation(expectation));
    }
    return JavaConverters.asScalaBufferConverter(expectationsList).asScala().toSeq();
  }

  public Expectation attachExpectation(Expectation expectation) throws FeatureStoreException, IOException {
    return attachExpectation(expectation.getName());
  }

  public Expectation attachExpectation(String name) throws FeatureStoreException, IOException {
    return expectationsApi.put(this, name);
  }

  public void detachExpectation(Expectation expectation) throws FeatureStoreException, IOException {
    detachExpectation(expectation.getName());
  }

  public void detachExpectation(String name) throws FeatureStoreException, IOException {
    expectationsApi.detach(this, name);
  }

  public void detachExpectations(scala.collection.Seq<Expectation> expectations)
          throws FeatureStoreException, IOException {
    for (Expectation expectation : (List<Expectation>) JavaConverters.seqAsJavaListConverter(expectations).asJava()) {
      expectationsApi.detach(this, expectation);
    }
  }

  /**
   * Update the FG validation type.
   * @param validationType validationType
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public void updateValidationType(ValidationType validationType) throws FeatureStoreException, IOException {
    this.validationType = validationType;
    featureGroupBaseEngine.updateValidationType(this);
  }

  @JsonIgnore
  public FeatureGroupValidation getValidation(Long time, DataValidationEngine.ValidationTimeType type)
          throws FeatureStoreException, IOException {
    return DataValidationEngine.getInstance().getValidation(this,
            new ImmutablePair<>(type, time));
  }

  /*
  public FeatureGroupValidation validate() throws FeatureStoreException, IOException {
    // Run data validation for entire feature group
    return validate(this.read());
  }

  protected FeatureGroupValidation validate(Dataset<Row> data) throws FeatureStoreException, IOException {
    return DataValidationEngine.getInstance().validate(data, this, expectationsApi.get(this));
  }

  public FeatureGroupValidation validateOnDemand(Dataset<Row> data) throws FeatureStoreException, IOException {
    return validate(data);
  }
  */

  @JsonIgnore
  public List<FeatureGroupValidation> getValidations() throws FeatureStoreException, IOException {
    return DataValidationEngine.getInstance().getValidations(this);
  }
}
