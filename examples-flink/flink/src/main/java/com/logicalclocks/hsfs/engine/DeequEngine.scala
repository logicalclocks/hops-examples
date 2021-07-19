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

package com.logicalclocks.hsfs.engine

/*
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckResult}
import com.amazon.deequ.constraints.{ConstrainableDataTypes, ConstraintResult}
import org.apache.spark.sql.DataFrame
*/

import java.util
import scala.collection.JavaConverters.{mapAsJavaMapConverter, _}

object DeequEngine {

  def longBoundary(min: Option[Double], max: Option[Double]): Long => Boolean = {
    (min, max) match {
      case (Some(x), Some(y)) => v => v >= x && v <= y
      case (Some(x), None) => _ >= x
      case (None, Some(y)) => _ <= y
      case _ => _ => true
    }
  }

  def doubleBoundary(min: Option[Double], max: Option[Double]): Double => Boolean = {
    (min, max) match {
      case (Some(x), Some(y)) => v => v >= x && v <= y
      case (Some(x), None) => _ >= x
      case (None, Some(y)) => _ <= y
      case _ => _ => true
    }
  }

  /*
  def addConstraint(check: Check, constraint: Constraint): Check = {
    constraint.name match {
      // Reason for using string instead of Enum is
      // https://stackoverflow.com/questions/7083502/why-cant-a-variable-be-a-stable-identifier
      case "HAS_MEAN" => check.hasMean(constraint.columns.get.head,
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "HAS_MIN" => check.hasMin(constraint.columns.get.head,
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "HAS_MAX" => check.hasMax(constraint.columns.get.head,
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "HAS_SUM" => check.hasSum(constraint.columns.get.head,
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "HAS_SIZE" => check.hasSize(
        longBoundary(constraint.min, constraint.max), constraint.hint)
      case "HAS_COMPLETENESS" => check.hasCompleteness(constraint.columns.get.head,
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "HAS_UNIQUENESS" => check.hasUniqueness(constraint.columns.get,
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "HAS_DISTINCTNESS" => check.hasDistinctness(constraint.columns.get,
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "HAS_UNIQUE_VALUE_RATIO" => check.hasUniqueValueRatio(constraint.columns.get,
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "HAS_NUMBER_OF_DISTINCT_VALUES" => check.hasNumberOfDistinctValues(constraint.columns.get.head,
        longBoundary(constraint.min, constraint.max), hint = constraint.hint)
      case "HAS_ENTROPY" => check.hasEntropy(constraint.columns.get.head,
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "HAS_MUTUAL_INFORMATION" => check.hasMutualInformation(constraint.columns.get.head, constraint.columns.get(1),
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "HAS_APPROX_QUANTILE" => check.hasApproxQuantile(constraint.columns.get.head, constraint.value.get.toDouble,
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "HAS_STANDARD_DEVIATION" => check.hasStandardDeviation(constraint.columns.get.head,
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "HAS_APPROX_COUNT_DISTINCT" => check.hasApproxCountDistinct(constraint.columns.get.head,
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "HAS_CORRELATION" => check.hasCorrelation(constraint.columns.get.head, constraint.columns.get(1),
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "HAS_PATTERN" => check.hasPattern(constraint.columns.get.head, constraint.pattern.get.r,
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "HAS_MIN_LENGTH" => check.hasMinLength(constraint.columns.get.head,
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "HAS_MAX_LENGTH" => check.hasMaxLength(constraint.columns.get.head,
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "HAS_DATATYPE" => check.hasDataType(constraint.columns.get.head,
        ConstrainableDataTypes.withName(constraint.acceptedType.get.name()),
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "IS_NON_NEGATIVE" => check.isNonNegative(constraint.columns.get.head,
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "IS_POSITIVE" => check.isPositive(constraint.columns.get.head,
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "IS_LESS_THAN" => check.isLessThan(constraint.columns.get.head, constraint.columns.get(1),
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "IS_LESS_THAN_OR_EQUAL_TO" => check.isLessThanOrEqualTo(constraint.columns.get.head, constraint.columns.get(1),
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "IS_GREATER_THAN" => check.isGreaterThan(constraint.columns.get.head, constraint.columns.get(1),
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "IS_GREATER_THAN_OR_EQUAL_TO" => check.isGreaterThanOrEqualTo(constraint.columns.get.head,
        constraint.columns.get(1), doubleBoundary(constraint.min, constraint.max), constraint.hint)
      case "IS_CONTAINED_IN" => check.isContainedIn(constraint.columns.get.head, constraint.legalValues.get,
        doubleBoundary(constraint.min, constraint.max), constraint.hint)
    }
  }

  def checksFromRules(constraintGroups: Seq[ConstraintGroup]): Seq[Check] = {
    constraintGroups
      .map(group => {
        var check = Check(CheckLevel.withName(group.level.toLowerCase().capitalize), group.description);
        group.constraints.foreach(constraint => check = addConstraint(check, constraint))
        check
      })
  }

  def runVerification(data: DataFrame, constraintGroups: Seq[ConstraintGroup]): util.Map[Check, CheckResult] = {
    val checks = checksFromRules(constraintGroups)
    VerificationSuite().onData(data).addChecks(checks).run().checkResults.asJava
  }

  def getConstraintResults(constraintResults : Seq[ConstraintResult]): util.List[ConstraintResult] ={
    constraintResults.asJava
  }
  */
}
