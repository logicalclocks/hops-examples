package io.hops.examples.spark.kafka;

import io.hops.util.HopsProducer;
import io.hops.util.HopsUtil;
import io.hops.util.SchemaNotFoundException;
import io.hops.util.spark.SparkConsumer;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount,
 * produces
 * hello world messages to Kafka using Hops Kafka Producer. Streaming code based
 * on Spark JavaDirectKafkaWordCount.
 * Usage: StreamingExample <type> <sink>
 * <type> type of kafka process (producer|consumer)
 * <sink> location in hdfs to append streaming output
 * <p>
 * Example:
 * $ bin/run-example streaming.StreamingExample
 * consumer /Projects/MyProject/Sink/Data
 * <p>
 */
public final class StreamingKafkaElastic {

  private static Logger LOGGER = Logger.getLogger(StreamingKafkaElastic.class.getName());
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(final String[] args) throws Exception {

    SparkConf sparkConf = new SparkConf().setAppName("StreamingExample");

    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
        Durations.seconds(2));
    //Use applicationId for sink folder
    final String appId = jssc.sparkContext().getConf().getAppId();

    //Get consumer groups
    SparkConsumer consumer = HopsUtil.getSparkConsumer(jssc);
    // Create direct kafka stream with topics
    JavaInputDStream<ConsumerRecord<String, String>> messages = consumer.
        createDirectStream();

    // Get the lines, split them into words, count the words and print
    JavaDStream<String> lines = messages.map(
        new Function<ConsumerRecord<String, String>, String>() {
      @Override
      public String call(ConsumerRecord<String, String> record) throws
          SchemaNotFoundException {
//        LOGGER.log(Level.INFO, "record1:" + record.key());
//        LOGGER.log(Level.INFO, "record2:" + record.value());
        return record.value();
      }
    });

    JavaDStream<String> words = lines.flatMap(
        new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String x) {
        return Arrays.asList(SPACE.split(x)).iterator();
      }
    });

   
    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
        new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String s) {
        return new Tuple2<>(s, 1);
      }
    }).reduceByKey(new Function2<Integer, Integer, Integer>() {
          @Override
          public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
          }
        });

    wordCounts.print();

    /*
     * Based on Spark Design patterns
     * http://spark.apache.org/docs/latest/streaming-programming-guide.html#output-operations-on-dstreams
     */
    wordCounts.foreachRDD(
        new VoidFunction2<JavaPairRDD<String, Integer>, Time>() {
      @Override
      public void call(JavaPairRDD<String, Integer> rdd, Time time) throws
          Exception {
        //Keep the latest microbatch output in the file
        rdd.repartition(1).saveAsHadoopFile(args[1] + "-" + appId,
            String.class,
            String.class,
            TextOutputFormat.class);
      }

    });

    /*
     * Enable this to get all the streaming outputs. It creates a folder for
     * every microbatch slot.
     * ///////////////////////////////////////////////////////////////////////
     * wordCounts.saveAsHadoopFiles(args[1], "txt", String.class,
     * String.class, (Class) TextOutputFormat.class);
     * ///////////////////////////////////////////////////////////////////////
     */
    // Start the computation
    jssc.start();
    jssc.awaitTermination();

  }

  public static boolean isShutdownRequested() {
    try {
      System.out.println("isshutdownrequested:" + HopsUtil.getProjectName());
      Configuration hdConf = new Configuration();
      Path hdPath = new org.apache.hadoop.fs.Path(
          "/Projects/" + HopsUtil.getProjectName() + "/Resources/.marker-producer-appId.txt");
      FileSystem hdfs = hdPath.getFileSystem(hdConf);
      return !hdfs.exists(hdPath);
    } catch (IOException ex) {
      Logger.getLogger(StreamingKafkaElastic.class.getName()).log(Level.SEVERE, null,
          ex);
    }
    return false;
  }
}
