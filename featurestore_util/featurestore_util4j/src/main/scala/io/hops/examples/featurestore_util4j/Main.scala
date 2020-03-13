package io.hops.examples.featurestore_util4j

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import io.hops.util.Hops
import io.hops.util.featurestore.FeaturestoreHelper
import org.rogach.scallop.ScallopConf
import play.api.libs.json._
import scala.collection.JavaConversions._
import scala.collection.JavaConversions
import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration;
import java.io.BufferedReader
import java.io.InputStreamReader
/**
  * Parser of command-line arguments
  */
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val input = opt[String](required = false, descr = "path to JSON input arguments", default = Some(""))
  verify()
}

/**
  * Program entry point
  *
  * This Scala program contains utility functions for starting
  * Apache Spark jobs for doing common operations in The Hopsworks Feature Store.
  * Such as updating feature group or training dataset statistics.
  */
object Main {

  /**
    * Main function, orchestrate the program
    *
    * @param args command line args
    */
  def main(args: Array[String]): Unit = {

    // Setup logging
    val log = LogManager.getLogger(Main.getClass.getName)
    log.setLevel(Level.INFO)
    log.info(s"Starting Sample Feature Engineering Job For Feature Store Examples")

    //Parse cmd arguments
    val hdfsConf = new Configuration()
    val conf = new Conf(args)
    val jsonArgs = parseInputJson(conf.input(), hdfsConf)
    val operation = jsonArgs("operation").as[String]
    log.info(s"---- Feature Store Util----- \n Operation: ${operation}")

    // Setup Spark
    var sparkConf: SparkConf = null
    sparkConf = sparkClusterSetup()
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext


    //Perform actions
    operation match {
      case "update_fg_stats" => updateFeaturegroupStats(jsonArgs, log)
      case "update_td_stats" => updateTrainingDatasetStats(jsonArgs, log)
      case "spark_sql_create_fg" => createFeaturegroupFromSparkSql(jsonArgs, log)
      case "jdbc_sql_create_fg" => createFeaturegroupFromJdbcSql(jsonArgs, log)
    }

    //Cleanup
    log.info("Shutting down spark job")
    spark.close
  }

  /**
    * Parse the input JSON arguments from HDFS
    *
    * @param inputPath path to the hdfs file
    * @param hdfsConf hdfs config
    * @return the parsed JSON arguments
    */
  def parseInputJson(inputPath: String, hdfsConf: Configuration): JsValue = {
    val filePath = new org.apache.hadoop.fs.Path(inputPath)
    val hdfs = filePath.getFileSystem(hdfsConf)
    if(!hdfs.exists(filePath))
      throw new IllegalArgumentException(s"Input argument path is not valid: ${inputPath}")
    val inputStream =hdfs.open(filePath)
    val reader = new BufferedReader(new InputStreamReader(inputStream))
    val jsonLines = reader.readLine()
    val parsedJson = Json.parse(jsonLines)
    return parsedJson
  }

  /**
    * Pre-process JSON with features into a list of feature names (strings)
    *
    * @param featuresJson the string to process
    * @return a list of features
    */
  def preProcessFeatures(featuresJson: JsValue): List[String] = {
    val featuresList = featuresJson.as[JsArray]
    if (featuresList.value.isEmpty)
      throw new IllegalArgumentException("Features cannot be empty")
    return featuresList.value.map((elem: JsValue) => elem("name").as[String]).toList
  }

  /**
    * Pre-process json featurestore string
    *
    * @param featurestoreJson the string to process
    * @return the featurestore string
    */
  def preProcessFeaturestore(featurestoreJson: JsValue): String = {
    val featurestoreStr = featurestoreJson.as[String]
    if (featurestoreStr.equals("")) {
      return Hops.getProjectFeaturestore.read
    } else {
      featurestoreStr
    }
  }

  /**
    * Pre-process featuregroups input JSON
    *
    * @param featuregroupsJson the json to process
    * @return map of featuregroup --> version
    */
  def preProcessFeatureGroups(featuregroupsJson: JsValue): java.util.Map[String, Integer] = {
    val featuregroupsList = featuregroupsJson.as[JsArray]
    if (featuregroupsList.value.isEmpty)
      throw new IllegalArgumentException("Feature Groups cannot be empty")
    val scalaFeaturegroupsMap: Map[String, Integer] = featuregroupsList.value.map((elem: JsValue) => {
      (elem("name").as[String], new Integer(elem("version").as[Int]))
    }).toList.toMap
    return JavaConversions.mapAsJavaMap(scalaFeaturegroupsMap)
  }

  /**
    * Pre-process the input jdbc json arguments
    *
    * @param jdbcArgumentsJson the input json to process
    * @return formatted JDBC arguments sub-string
    */
  def preProcessJdbcArguments(jdbcArgumentsJson: JsValue): String = {
    val jdbcArgumentsList = jdbcArgumentsJson.as[JsArray]
    if (!jdbcArgumentsList.value.isEmpty) {
      val hopsTrustStore = Hops.getTrustStore()
      val hopsKeyStore = Hops.getKeyStore()
      val pw = Hops.getKeystorePwd()
      val jdbcArgsStr = jdbcArgumentsList.value.map((argumentValue: JsValue) => {
        val argumentValueArr = argumentValue.as[String].split(",")
        val argument = argumentValueArr(0)
        val value = argumentValueArr(1)
        argument match {
          case "sslTrustStore" => "sslTrustStore=" + hopsTrustStore + ";"
          case "trustStorePassword" => "trustStorePassword=" + pw + ";"
          case "sslKeyStore" => "sslKeyStore=" + hopsKeyStore + ";"
          case "keyStorePassword" => "keyStorePassword=" + pw + ";"
        }
      }).mkString("")
      return jdbcArgsStr
    } else {
      return ""
    }
  }

  /**
    * Creates a Feature Group in the featurestore based on the result of a SparkSQL query (as specified in the
    * command-line arguments).
    *
    * @param jsonArgs the input json arguments
    * @param log  logger
    */
  def createFeaturegroupFromSparkSql(jsonArgs: JsValue, log: Logger): Unit = {
    //Parse arguments
    val sqlQuery = jsonArgs("sqlQuery").as[String]
    val hiveDb = jsonArgs("hiveDatabase").as[String]
    val featuregroup = jsonArgs("featuregroup").as[String]
    val description = jsonArgs("description").as[String]
    val version = jsonArgs("version").as[Int]
    val descriptiveStats = jsonArgs("descriptiveStats").as[Boolean]
    val featureCorrelation = jsonArgs("featureCorrelation").as[Boolean]
    val clusterAnalysis = jsonArgs("clusterAnalysis").as[Boolean]
    val featureHistograms = jsonArgs("featureHistograms").as[Boolean]
    val statColumns = jsonArgs("statColumns").as[List[String]]
    val featurestoreToQuery = preProcessFeaturestore(jsonArgs("featurestore"))
    val online = jsonArgs("online").as[Boolean]

    //Run SparkSQL Command
    log.info(s"Running SQL Command: ${sqlQuery} against database: ${hiveDb}")
    val spark = Hops.findSpark()
    spark.sql("use " + hiveDb)
    val resultDf = spark.sql(sqlQuery)

    //Create Feature Group of the Results
    log.info(s"Creating Feature Group ${featuregroup}")
    Hops.createFeaturegroup(featuregroup)
      .setDataframe(resultDf)
      .setFeaturestore(featurestoreToQuery)
      .setDescriptiveStats(descriptiveStats)
      .setFeatureCorr(featureCorrelation)
      .setFeatureHistograms(featureHistograms)
      .setClusterAnalysis(clusterAnalysis)
      .setStatColumns(statColumns)
      .setDescription(description)
      .setOnline(online)
      .setVersion(version).write()
  }


  /**
    * Creates a Feature Group in the featurestore based on the result of a JDBC Sql query (as specified in the
    * command-line arguments).
    *
    * @param jsonArgs the json input arguments
    * @param log  logger
    */
  def createFeaturegroupFromJdbcSql(jsonArgs: JsValue, log: Logger): Unit = {
    //Parse arguments
    val sqlQuery = jsonArgs("sqlQuery").as[String]
    val jdbcString = jsonArgs("jdbcString").as[String]
    val jdbcArguments = preProcessJdbcArguments(jsonArgs("jdbcArguments"))
    val featuregroup = jsonArgs("featuregroup").as[String]
    val description = jsonArgs("description").as[String]
    val version = jsonArgs("version").as[Int]
    val descriptiveStats = jsonArgs("descriptiveStats").as[Boolean]
    val featureCorrelation = jsonArgs("featureCorrelation").as[Boolean]
    val clusterAnalysis = jsonArgs("clusterAnalysis").as[Boolean]
    val featureHistograms = jsonArgs("featureHistograms").as[Boolean]
    val statColumns = jsonArgs("statColumns").as[List[String]]
    val featurestoreToQuery = preProcessFeaturestore(jsonArgs("featurestore"))
    val online = jsonArgs("online").as[Boolean]

    //Setup JDBC
    log.info(s"Setting up JDBC")
    FeaturestoreHelper.registerCustomJdbcDialects
    var driver = ""
    if(jdbcString.startsWith("jdbc:hive2") || jdbcString.contains("hive2")){
      driver = "org.apache.hive.jdbc.HiveDriver"
    }
    //Open JDBC Connection and make SQL Query
    log.info(s"Running SQL Command: ${sqlQuery} against database: ${jdbcString}")
    val spark = Hops.findSpark()
    val resultDf = spark.read.format("jdbc")
      .option("url", jdbcString + jdbcArguments)
      .option("driver", driver)
      .option("dbtable", "(" + sqlQuery + ") fs_q").load()

    //Remove alias from column names
    log.info(s"Removing alias from column names")
    val schemaNames = resultDf.schema.map((field) => field.name.replace("fs_q.", ""))
    val castedDf =  resultDf.toDF(schemaNames: _*)

    //Create Feature Group
    log.info(s"Creating Feature Group ${featuregroup}")
    Hops.createFeaturegroup(featuregroup)
      .setDataframe(castedDf)
      .setFeaturestore(featurestoreToQuery)
      .setDescriptiveStats(descriptiveStats)
      .setFeatureCorr(featureCorrelation)
      .setFeatureHistograms(featureHistograms)
      .setClusterAnalysis(clusterAnalysis)
      .setStatColumns(statColumns)
      .setDescription(description)
      .setOnline(online)
      .setVersion(version).write()

  }

  /**
    * Updates featuregroup statistics based on command-line arguments
    *
    * @param jsonArgs the json arguments
    * @param log  logger
    */
  def updateFeaturegroupStats(jsonArgs: JsValue, log: Logger): Unit = {
    val featuregroup = jsonArgs("featuregroup").as[String]
    val version = jsonArgs("version").as[Int]
    val featurestoreToQuery = preProcessFeaturestore(jsonArgs("featurestore"))
    val descriptiveStats = jsonArgs("descriptiveStats").as[Boolean]
    val featureCorrelation = jsonArgs("featureCorrelation").as[Boolean]
    val clusterAnalysis = jsonArgs("clusterAnalysis").as[Boolean]
    val featureHistograms = jsonArgs("featureHistograms").as[Boolean]
    val statColumns = jsonArgs("statColumns").as[List[String]]

    log.info(s"Updating Feature Group Statistics for Feature Group: ${
      featuregroup
    }")

    Hops.updateFeaturegroupStats(featuregroup)
      .setFeaturestore(featurestoreToQuery)
      .setVersion(version)
      .setDescriptiveStats(descriptiveStats)
      .setFeatureCorr(featureCorrelation)
      .setFeatureHistograms(featureHistograms)
      .setClusterAnalysis(clusterAnalysis)
      .setStatColumns(statColumns)
      .write()

    log.info(s"Statistics updated successfully")
  }

  /**
    * Updates training dataset statistics based on command-line arguments
    *
    * @param jsonArgs input JSON arguments
    * @param log  logger
    */
  def updateTrainingDatasetStats(jsonArgs: JsValue, log: Logger): Unit = {
    val trainingDataset = jsonArgs("trainingDataset").as[String]
    val version = jsonArgs("version").as[Int]
    val featurestoreToQuery = preProcessFeaturestore(jsonArgs("featurestore"))
    val descriptiveStats = jsonArgs("descriptiveStats").as[Boolean]
    val featureCorrelation = jsonArgs("featureCorrelation").as[Boolean]
    val clusterAnalysis = jsonArgs("clusterAnalysis").as[Boolean]
    val featureHistograms = jsonArgs("featureHistograms").as[Boolean]
    val statColumns = jsonArgs("statColumns").as[List[String]]

    log.info(s"Update Training Dataset Stats")

    Hops.updateTrainingDatasetStats(trainingDataset)
      .setFeaturestore(featurestoreToQuery)
      .setVersion(version)
      .setDescriptiveStats(descriptiveStats)
      .setFeatureCorr(featureCorrelation)
      .setFeatureHistograms(featureHistograms)
      .setClusterAnalysis(clusterAnalysis)
      .setStatColumns(statColumns)
      .write()

    log.info(s"Training Dataset Stats updated Successfully")
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
