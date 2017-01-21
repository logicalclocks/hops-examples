package io.hops.examples.spark;

import java.util.Arrays;
import java.util.Iterator;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

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
public final class StructuredStreaming {

  public static void main(final String[] args) throws Exception {
    SparkSession spark = SparkSession
            .builder()
            .appName("HopsJavaStructuredNetworkWordCount")
            .getOrCreate();

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    Dataset<Row> lines = spark
            .readStream()
            .format("socket")
            .option("host", args[0])
            .option("port", args[1])
            .load();

    // Split the lines into words
    Dataset<String> words = lines
            .as(Encoders.STRING())
            .flatMap(new FlatMapFunction<String, String>() {
              @Override
              public Iterator<String> call(String x) {
                return Arrays.asList(x.split(" ")).iterator();
              }
            }, Encoders.STRING());

    // Generate running word count
    Dataset<Row> wordCounts = words.groupBy("value").count();
    // Start running the query that prints the running counts to the console
    StreamingQuery query = wordCounts.writeStream()
            .outputMode("complete")
            .format("console")
            .start();

    query.awaitTermination();

  }

}
