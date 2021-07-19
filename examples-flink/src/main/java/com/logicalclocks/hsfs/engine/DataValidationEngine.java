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

package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;
import com.logicalclocks.hsfs.metadata.FeatureGroupValidation;
import com.logicalclocks.hsfs.metadata.FeatureGroupValidationsApi;
import com.logicalclocks.hsfs.metadata.validation.Rule;
import com.logicalclocks.hsfs.metadata.validation.RuleName;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.IOException;
import java.util.List;

public class DataValidationEngine {

  private static DataValidationEngine INSTANCE = null;

  public static synchronized DataValidationEngine getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new DataValidationEngine();
    }
    return INSTANCE;
  }

  private final FeatureGroupValidationsApi featureGroupValidationsApi =
      new FeatureGroupValidationsApi(EntityEndpointType.FEATURE_GROUP);

  public List<FeatureGroupValidation> getValidations(FeatureGroupBase featureGroupBase)
      throws FeatureStoreException, IOException {
    return featureGroupValidationsApi.get(featureGroupBase);
  }

  public FeatureGroupValidation getValidation(FeatureGroupBase featureGroupBase, ImmutablePair<ValidationTimeType,
                                              Long> pair)
      throws FeatureStoreException, IOException {
    return featureGroupValidationsApi.get(featureGroupBase, pair);
  }

  public RuleName getRuleNameFromDeequ(String rule) {
    switch (rule.toLowerCase()) {
      case "maximum":
        return RuleName.HAS_MAX;
      case "minimum":
        return RuleName.HAS_MIN;
      case "mean":
        return RuleName.HAS_MEAN;
      case "size":
        return RuleName.HAS_SIZE;
      case "sum":
        return RuleName.HAS_SUM;
      case "completeness":
        return RuleName.HAS_COMPLETENESS;
      case "uniqueness":
        return RuleName.HAS_UNIQUENESS;
      case "distinctness":
        return RuleName.HAS_DISTINCTNESS;
      case "uniquevalueratio":
        return RuleName.HAS_UNIQUE_VALUE_RATIO;
      case "histogram":
        return RuleName.HAS_NUMBER_OF_DISTINCT_VALUES;
      case "entropy":
        return RuleName.HAS_ENTROPY;
      case "mutualinformation":
        return RuleName.HAS_MUTUAL_INFORMATION;
      case "approxquantile":
        return RuleName.HAS_APPROX_QUANTILE;
      case "standarddeviation":
        return RuleName.HAS_STANDARD_DEVIATION;
      case "approxcountdistinct":
        return RuleName.HAS_APPROX_COUNT_DISTINCT;
      case "correlation":
        return RuleName.HAS_CORRELATION;
      case "patternmatch":
        return RuleName.HAS_PATTERN;
      case "minlength":
        return RuleName.HAS_MIN_LENGTH;
      case "maxlength":
        return RuleName.HAS_MAX_LENGTH;
      case "datatype":
        return RuleName.HAS_DATATYPE;
      case "isnonnegative":
        return RuleName.IS_NON_NEGATIVE;
      case "ispositive":
        return RuleName.IS_POSITIVE;
      case "islessthan":
        return RuleName.IS_LESS_THAN;
      case "islessthanorequalto":
        return RuleName.IS_LESS_THAN_OR_EQUAL_TO;
      case "isgreaterthan":
        return RuleName.IS_GREATER_THAN;
      case "isgreaterthanorequalto":
        return RuleName.IS_GREATER_THAN_OR_EQUAL_TO;
      case "iscontainedin":
        return RuleName.IS_CONTAINED_IN;

      default:
        throw new UnsupportedOperationException("Deequ rule not supported: " + rule);
    }
  }

  public boolean isRuleAppliedToFeaturePairs(Rule rule) {
    return rule.getName() == RuleName.IS_GREATER_THAN_OR_EQUAL_TO
      || rule.getName() == RuleName.IS_GREATER_THAN
      || rule.getName() == RuleName.IS_LESS_THAN
      || rule.getName() == RuleName.IS_LESS_THAN_OR_EQUAL_TO
      || rule.getName() == RuleName.HAS_MUTUAL_INFORMATION
      || rule.getName() == RuleName.HAS_CORRELATION;
  }

  public enum ValidationTimeType {
    VALIDATION_TIME,
    COMMIT_TIME
  }

}
