package io.hops.examples.spark.kafka

import io.hops.util.Hops
import org.apache.spark.sql.avro._
import org.apache.spark.sql._
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf


/**
  * Example that uses HopsUtil and Spark-Kafka structured streaming with Avro.
  * Runs a producer and a consumer, depending on the user-provided argument.
  *
  */
object StructuredStreamingKafka {

  def main(args: Array[String]): Unit = {
    // Setup logging
    val log = LogManager.getLogger(StructuredStreamingKafka.getClass)
    log.setLevel(Level.INFO)
    log.info(s"Starting Structured Streaming Kafka with Avro Job")

    if (args.length != 2) {
      log.error("Program expects at two arguments, type<producer|consumer> and a topic name")
      System.exit(1)
    } else if (args(0) != "producer" && args(0) != "consumer") {
      log.error("Valid types are <producer|consumer>")
      System.exit(1)
    }

    // Setup Spark
    var sparkConf: SparkConf = new SparkConf()
    val spark = SparkSession.builder().appName(Hops.getJobName).config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val kafkaClientType = args(0)
    val topic = args(1)

    if (args(0) == "producer") {
      import spark.implicits._
      //First prepare some random input data
      val data = Seq(
        Row(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS").format(new Date), "INFO", "org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl", "Container container_e01_1494850115055_0016_01_000002 succeeded"),
        Row(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS").format(new Date), "DEBUG", "org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.AppLogAggregatorImpl", "Cannot create writer for app application_1494433225517_0008. Skip log upload this time."),
        Row(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS").format(new Date), "WARN", "org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl", "\"Sending out 2 container statuses: \"\n          + \"[ContainerStatus: [ContainerId: container_e01_1494850115055_0016_01_000001, State: RUNNING, \"\n          + \"Diagnostics: , ExitStatus: -1000, ], \"\n          + \"ContainerStatus: [ContainerId: container_e01_1494850115055_0016_01_000002, \"\n+ \"State: RUNNING, Diagnostics: , ExitStatus: -1000, ]]\""),
        Row(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS").format(new Date), "INFO", "org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl", "Container container_e01_1494850115055_0016_01_000002 succeeded")
      )

      //Schema corresponds to the schema in the Hops Kafka Schema Registry
      val schema = StructType(
        Array(
          StructField("timestamp", StringType, nullable = false),
          StructField("priority", StringType, nullable = false),
          StructField("logger", StringType, nullable = false),
          StructField("message", StringType, nullable = false)
        )
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        schema
      )

      //Send df with input data to the Kafka topic in Avro format
      df.select(to_avro(struct(df.columns.map(column): _*)).alias("value")).
        write.format("kafka").
        option("kafka.bootstrap.servers", Hops.getBrokerEndpoints).
        option("kafka.security.protocol", "SSL").
        option("kafka.ssl.truststore.location", Hops.getTrustStore).
        option("kafka.ssl.truststore.password", Hops.getKeystorePwd).
        option("kafka.ssl.keystore.location", Hops.getKeyStore).
        option("kafka.ssl.keystore.password", Hops.getKeystorePwd).
        option("kafka.ssl.key.password", Hops.getKeystorePwd).
        option("kafka.ssl.endpoint.identification.algorithm", "").
        option("topic", topic).save()


    } else {
      val dfFromKafka = spark.readStream.format("kafka").
        option("kafka.bootstrap.servers", Hops.getBrokerEndpoints).
        option("subscribe", topic).
        option("startingOffsets", "earliest").
        option("kafka.security.protocol", "SSL").
        option("kafka.ssl.truststore.location", Hops.getTrustStore).
        option("kafka.ssl.truststore.password", Hops.getKeystorePwd).
        option("kafka.ssl.keystore.location", Hops.getKeyStore).
        option("kafka.ssl.keystore.password", Hops.getKeystorePwd).
        option("kafka.ssl.key.password", Hops.getKeystorePwd).
        option("kafka.ssl.endpoint.identification.algorithm", "").
        load()

      val logsDF = dfFromKafka.select(from_avro(col("value"), Hops.getSchema(topic)) as 'logs)

      logsDF.writeStream.format("parquet").
        option("path", "/Projects/" + Hops.getProjectName + "/Resources/" + topic + "-parquet").
        option("checkpointLocation", "/Projects/" + Hops.getProjectName + "/Resources/" + topic + "-checkpoint").
        start().awaitTermination()

    }

    log.info("Shutting down spark job")
    spark.close
  }


}
