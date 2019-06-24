package io.hops.examples.featurestore_tour

import io.hops.examples.featurestore_tour.featuregroups.ComputeFeatures
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import io.hops.util.Hops
import org.rogach.scallop.ScallopConf

/**
  * Parser of command-line arguments
  */
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val input = opt[String](required = false, descr = "path to input sample data files (csv files)")
  verify()
}

/**
 * Program entry point
 *
 * Sample Feature Engineering Job for the Hopsworks Feature
 */
object Main {

  def main(args: Array[String]): Unit = {

    // Setup logging
    val log = LogManager.getLogger(Main.getClass.getName)
    log.setLevel(Level.INFO)
    log.info(s"Starting Sample Feature Engineering Job For Feature Store Examples")

    //Parse cmd arguments
    val conf = new Conf(args)
    val input = parseInput(conf.input())

    // Setup Spark
    var sparkConf: SparkConf = null
    sparkConf = sparkClusterSetup()
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    ComputeFeatures.computeGamesFeatureGroup(spark, log, input)
    ComputeFeatures.computeSeasonScoresFeatureGroup(spark, log, input)
    ComputeFeatures.computeAttendanceFeatureGroup(spark, log, input)
    ComputeFeatures.computePlayersFeatureGroup(spark, log, input)
    ComputeFeatures.computeTeamsFeatureGroup(spark, log, input)

    log.info("Shutting down spark job")
    spark.close
  }

  /**
    * Expands input path with "hdfs:///Projects/projectName/" in case the user supplied a relative path
    *
    * @param input the path to expand
    * @return the expanded path
    */
  def parseInput(input: String): String = {
    if (input.contains("hdfs://")){
      return input
    }  else {
      return "hdfs:///Projects/" + Hops.getProjectName + "/" + input
    }
  }

  /**
   * Hard coded settings for local spark training
   *
   * @return spark configurationh
   */
  def localSparkSetup(): SparkConf = {
    new SparkConf().setAppName("feature_engineering_spark").setMaster("local[*]")
  }

  /**
   * Hard coded settings for cluster spark training
   *
   * @return spark configuration
   */
  def sparkClusterSetup(): SparkConf = {
    new SparkConf().setAppName("feature_engineering_spark")
  }

}
