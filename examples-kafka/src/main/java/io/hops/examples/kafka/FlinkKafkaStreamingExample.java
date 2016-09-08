package io.hops.examples.kafka;

import io.hops.kafkautil.HopsKafkaUtil;
import io.hops.kafkautil.flink.HopsFlinkKafkaProducer;
import java.util.Arrays;
import java.util.Map;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.fs.DateTimeBucketer;
import org.apache.flink.streaming.connectors.fs.RollingSink;

/**
 *
 * @author teo
 */
public class FlinkKafkaStreamingExample {

  public static void main(String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    if (parameterTool.getNumberOfParameters() < 2 || !parameterTool.has("topic")) {
      System.out.println(
              "Missing parameters!\nUsage: --topic <topic> --sink_path <rolling sink path>");
      System.exit(1);
    }
    System.out.println("FlinkKafkaStreamingExample.Params:"+parameterTool.toMap().toString());
    HopsKafkaUtil hopsKafkaUtil = HopsKafkaUtil.getInstance();
    Map<String, String> kafkaProps = hopsKafkaUtil.getKafkaProps(
            parameterTool.get("kafka_params"));
    hopsKafkaUtil.setup(kafkaProps.get(hopsKafkaUtil.KAFKA_SESSIONID_ENV_VAR),
            Integer.parseInt(kafkaProps.get(
                    hopsKafkaUtil.KAFKA_PROJECTID_ENV_VAR)),
            parameterTool.get("topic"),
            parameterTool.get("domain"),
            kafkaProps.get(hopsKafkaUtil.KAFKA_BROKERADDR_ENV_VAR),
            parameterTool.get("url"),
            kafkaProps.get(hopsKafkaUtil.KAFKA_K_CERTIFICATE_ENV_VAR),
            kafkaProps.get(hopsKafkaUtil.KAFKA_T_CERTIFICATE_ENV_VAR));

    if (parameterTool.get("type").equalsIgnoreCase("producer")) {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.
              getExecutionEnvironment();
      env.getConfig().disableSysoutLogging();
      env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4,
              10000));

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
      HopsFlinkKafkaProducer producer = new HopsFlinkKafkaProducer(
              parameterTool.get("topic"),
              new HopsAvroSchema(parameterTool.get("topic")));
      messageStream.addSink(producer);

      env.execute("Write into Kafka example");
    } else {

      StreamExecutionEnvironment env = StreamExecutionEnvironment.
              getExecutionEnvironment();
      env.getConfig().disableSysoutLogging();
      env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4,
              10000));
      //env.enableCheckpointing(5000); // create a checkpoint every 5 secodns
      //Get user parameters, excluding kafka ones set by HopsWorks
      // make parameters available in the web interface
      env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(
              Arrays.copyOf(args, args.length-2))); 
      
      DataStream<String> messageStream = env
              .addSource(hopsKafkaUtil.getHopsFlinkKafkaConsumer(parameterTool.
                      get("topic"),
                      new HopsAvroSchema(parameterTool.get("topic"))));

      RollingSink<String> rollingSink = new RollingSink<>(
              parameterTool.get("sink_path"));
      //Size of part file in bytes
      int batchSize = 8;
      if(parameterTool.has("batch_size")){
       batchSize = Integer.parseInt(parameterTool.get("batch_size")); 
      }
      rollingSink.setBatchSize(1024 *  batchSize);
      rollingSink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HH"));
      messageStream.addSink(rollingSink);

      // write kafka stream to standard out.
      messageStream.print();

      env.execute("Read from Kafka example");
    }
  }
}
