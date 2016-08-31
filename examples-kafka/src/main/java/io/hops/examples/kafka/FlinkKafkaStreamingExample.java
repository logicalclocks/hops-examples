package io.hops.examples.kafka;

import io.hops.kafkautil.HopsKafkaUtil;
import io.hops.kafkautil.flink.HopsFlinkKafkaProducer;
import java.util.Arrays;
import java.util.Map;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 *
 * @author teo
 */
public class FlinkKafkaStreamingExample {

  public static void main(String[] args) throws Exception {
//		ParameterTool parameterTool = ParameterTool.fromArgs(args);
//		if(parameterTool.getNumberOfParameters() < 2) {
//			System.out.println("Missing parameters!\nUsage: Kafka --topic <topic> --bootstrap.servers <kafka brokers>");
//			System.exit(1);
//		}

    StreamExecutionEnvironment env = StreamExecutionEnvironment.
            getExecutionEnvironment();
    env.getConfig().disableSysoutLogging();
    env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4,
            10000));

    // very simple data generator
    DataStream<String> messageStream = env.addSource(
            new SourceFunction<String>() {
      public boolean running = true;

      @Override
      public void run(SourceContext<String> ctx) throws Exception {
        long i = 0;
        while (this.running) {
          ctx.collect("Element - " + i++);
          Thread.sleep(500);
        }
      }

      @Override
      public void cancel() {
        running = false;
      }
    });

    HopsKafkaUtil hopsKafkaUtil = HopsKafkaUtil.getInstance();

    System.out.println("KAFKA-ARGS:" + Arrays.toString(args));
    Map<String, String> kafkaProps = hopsKafkaUtil.getKafkaProps(
            args[args.length - 1]);
    for (Map.Entry<String, String> entry : kafkaProps.entrySet()) {
      System.out.println("KAFKA-PROP:" + entry.getKey() + "," + entry.
              getValue());
    }
    hopsKafkaUtil.setup(kafkaProps.get(hopsKafkaUtil.KAFKA_SESSIONID_ENV_VAR),
            Integer.parseInt(kafkaProps.get(
                    hopsKafkaUtil.KAFKA_PROJECTID_ENV_VAR)),
            args[0],
            "localhost",
            kafkaProps.get(hopsKafkaUtil.KAFKA_BROKERADDR_ENV_VAR),
            "http://localhost:8080",
            "/srv/glassfish/domain1/config/" + kafkaProps.get(
                    hopsKafkaUtil.KAFKA_K_CERTIFICATE_ENV_VAR),
            "/srv/glassfish/domain1/config/" + kafkaProps.get(
                    hopsKafkaUtil.KAFKA_T_CERTIFICATE_ENV_VAR));
    // write data into Kafka
    //messageStream.addSink(new FlinkKafkaProducer08<>(parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties()));
    HopsFlinkKafkaProducer producer = new HopsFlinkKafkaProducer<>(args[0], 
            new SimpleStringSchema(), null);
    messageStream.addSink(producer);

    env.execute("Write into Kafka example");
  }
}
