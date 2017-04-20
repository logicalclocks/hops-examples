package io.hops.examples.spark.kafka;

import com.twitter.bijection.Injection;
import io.hops.util.HopsUtil;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.ProcessingTime;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 *
 * @author tkak
 */
public class StructuredStreamingKafka {

  private static final Map<String, Injection<GenericRecord, byte[]>> RECORD_INJECTIONS = HopsUtil.getRecordInjections();
  private static final Logger LOG = Logger.getLogger(StructuredStreamingKafka.class.getName());

  public static void main(String[] args) throws StreamingQueryException {

    // Create DataSet representing the stream of input lines from kafka
    DataStreamReader dsr = HopsUtil.getSparkConsumer().getKafkaDataStreamReader();
    Dataset<Row> lines = dsr.load();

    // Generate running word count
    Dataset<String> wordCounts = lines
        .flatMap((Row record) -> {
          StringBuilder line = new StringBuilder();
          GenericRecord genericRecord = RECORD_INJECTIONS.entrySet().iterator().next().getValue().invert(record.getAs(
              "value")).get();
          line.append(genericRecord.get("platform").toString())
              .append(" ")
              .append(genericRecord.get("program").toString());
          return Arrays.asList(line.toString().split(" ")).iterator();
        }, Encoders.STRING());
    Dataset<Row> counts = wordCounts.groupBy("value").count();

    // Start running the query that prints the running counts to the console
    StreamingQuery query = counts.writeStream()
        .format("console")
        .outputMode("complete")
        .option("checkpointLocation", "/Projects/" + HopsUtil.getProjectName() + "/Resources/checkpoint-" + HopsUtil.
            getAppId())
        .trigger(new ProcessingTime(1000))
        .start();

    query.awaitTermination();
  }

}
