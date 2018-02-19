package io.hops.examples.flink.kafka;

import io.hops.util.Constants;
import io.hops.util.Hops;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.fs.DateTimeBucketer;
import org.apache.flink.streaming.connectors.fs.RollingSink;

/**
 * A simple streaming application ths uses the Hops Kafka Utility to
 * produce and consume streams from Kafka.
 * <p>
 * <p>
 */
public class StreamingExample {

  private static final Logger LOG = Logger.getLogger(StreamingExample.class.getName());

  public static void main(String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    if (parameterTool.getNumberOfParameters() < 2 || !parameterTool.has("type")) {
      LOG.log(Level.SEVERE, "Missing parameters!\nUsage: -type <producer|consumer> "
          + "[-sink_path <rolling_sink path>]"
          + " [-batch_size <rolling_file_size>]"
          + " [-bucket_format <bucket_format>]");
      throw new Exception(
          "Missing parameters!\nUsage: -type <producer|consumer> "
          + "[-sink_path <rolling_sink path>]"
          + " [-batch_size <rolling_file_size>]"
          + " [-bucket_format <bucket_format>]");
    }
    LOG.log(Level.INFO, "FlinkKafkaStreamingExample.Params:{0}", parameterTool.toMap().toString());

    ////////////////////////////////////////////////////////////////////////////
    //Hopsworks utility method to automatically set parameters for Kafka
    Hops.setup(Hops.getFlinkKafkaProps(parameterTool.get(
        Constants.KAFKA_FLINK_PARAMS)));
    ////////////////////////////////////////////////////////////////////////////
    if (parameterTool.get("type").equalsIgnoreCase("producer")) {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.
          getExecutionEnvironment();
      env.getConfig().disableSysoutLogging();
      env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));

      // very simple data generator
      DataStream<Tuple4<String, String, String, String>> messageStream = env.
          addSource(
              new SourceFunction<Tuple4<String, String, String, String>>() {
            public boolean running = true;

            @Override
            public void run(
                SourceContext<Tuple4<String, String, String, String>> ctx)
                throws Exception {
              long i = 0;
              while (this.running) {
                ctx.collect(new Tuple4("platform", "HopsWorks",
                    "program", "Flink Streaming - " + i++));
                Thread.sleep(500);
              }
            }

            @Override
            public void cancel() {
              running = false;
            }
          });

      // write data into Kafka
      for (String topic : Hops.getTopics()) {
        messageStream.addSink(Hops.getFlinkProducer(topic));
      }
      env.execute("Write into Kafka example");
    } else {

      StreamExecutionEnvironment env = StreamExecutionEnvironment.
          getExecutionEnvironment();
      env.getConfig().disableSysoutLogging();
      env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
      //env.enableCheckpointing(5000); // create a checkpoint every 5 secodns
      //Get user parameters, excluding kafka ones set by HopsWorks
      // make parameters available in the web interface
      env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(
          Arrays.copyOf(args, args.length - 2)));

      for (String topic : Hops.getTopics()) {
        DataStream<String> messageStream = env.addSource(Hops.
            getFlinkConsumer(topic));
        String dateTimeBucketerFormat = "yyyy-MM-dd--HH";
        if (parameterTool.has("sink_path")) {
          if (parameterTool.has("bucket_format")) {
            if (parameterTool.has("bucket_format")) {
              dateTimeBucketerFormat = parameterTool.get("bucket_format");
            }
          }
          RollingSink<String> rollingSink = new RollingSink<>(
              parameterTool.get("sink_path"));
          //Size of part file in bytes
          int batchSize = 8;
          if (parameterTool.has("batch_size")) {
            batchSize = Integer.parseInt(parameterTool.get("batch_size"));
          }
          rollingSink.setBatchSize(1024 * batchSize);
          rollingSink.setBucketer(new DateTimeBucketer(dateTimeBucketerFormat));
          messageStream.addSink(rollingSink);
        }
        // write kafka stream to standard out.
        messageStream.print();
      }
      env.execute("Read from Kafka example");
    }
  }
}
