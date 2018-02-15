package io.hops.examples.spark.kafka;

import com.twitter.bijection.Injection;
import io.hops.util.HopsUtil;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

/**
 *
 * <p>
 */
public class IntrabodyClient {

  private static final Map<String, Injection<GenericRecord, byte[]>> RECORD_INJECTIONS = HopsUtil.getRecordInjections();
  private static final Logger logger = Logger.getLogger(StructuredStreamingKafka.class.getName());

  public static void main(String[] args) throws StreamingQueryException, InterruptedException {
    // Create DataSet representing the stream of input lines from kafka
    DataStreamReader dsr = HopsUtil.getSparkConsumer().getKafkaDataStreamReader();
    Dataset<Row> lines = dsr.load();

    // Generate running word count
    Dataset<IntrabodyMessage> messages = lines
        .map(new MapFunction<Row, IntrabodyMessage>() {
          @Override
          public IntrabodyMessage call(Row record) throws Exception {
            GenericRecord genericRecord = RECORD_INJECTIONS.entrySet().iterator().next().getValue().invert(record.
                getAs("value")).get();
            IntrabodyMessage message = new IntrabodyMessage(
                genericRecord.get("recordUUID").toString(),
                genericRecord.get("epochMillis").toString(),
                genericRecord.get("deviceUUID").toString(),
                genericRecord.get("bootNum").toString(),
                genericRecord.get("bootMillis").toString(),
                genericRecord.get("longitude").toString(),
                genericRecord.get("latitude").toString());
            return message;
          }
        }, Encoders.bean(IntrabodyMessage.class));

    Dataset<String> messagesRaw = lines
        .map(new MapFunction<Row, String>() {
          @Override
          public String call(Row record) throws Exception {
            GenericRecord genericRecord = RECORD_INJECTIONS.entrySet().iterator().next().getValue().invert(record.
                getAs("value")).get();

            return genericRecord.toString();
          }
        }, Encoders.STRING());

    // Start running the query that prints the running counts to the console
    StreamingQuery queryFile = messages.writeStream()
        .format("parquet")
        .option("path", "/Projects/" + HopsUtil.getProjectName() + "/Resources/data-parquet-" + HopsUtil.getAppId())
        .option("checkpointLocation", "/Projects/" + HopsUtil.getProjectName() + "/Resources/checkpoint-parquet-"
            + HopsUtil.getAppId())
        .trigger(Trigger.ProcessingTime(10000))
        .start();

    StreamingQuery queryFile2 = messagesRaw.writeStream()
        .format("text")
        .option("path", "/Projects/" + HopsUtil.getProjectName() + "/Resources/data-text-" + HopsUtil.getAppId())
        .option("checkpointLocation", "/Projects/" + HopsUtil.getProjectName() + "/Resources/checkpoint-text-"
            + HopsUtil.getAppId())
        .trigger(Trigger.ProcessingTime(10000))
        .start();

    HopsUtil.shutdownGracefully(queryFile);
  }
}
