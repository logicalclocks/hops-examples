package io.hops.examples.kafka;

import io.hops.kafkautil.HopsKafkaConsumer;
import io.hops.kafkautil.HopsKafkaProducer;
import io.hops.kafkautil.HopsKafkaUtil;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * <p>
 */
public class FlinkKafkaExample {

  private static int BOUND = 100;

  // *************************************************************************
  // PROGRAM
  // *************************************************************************
  public static void main(String[] args) throws Exception {

    // Checking input parameters
    //final ParameterTool params = ParameterTool.fromArgs(args);
    System.out.println("  Usage: IterateExample --input <path> --output <path>");
//    if (params.has("bound")) {
//      BOUND = Integer.parseInt(params.get("bound"));
//    }
    // set up input for the stream of integer pairs

    // obtain execution environment and set setBufferTimeout to 1 to enable
    // continuous flushing of the output buffers (lowest latency)
    StreamExecutionEnvironment env = StreamExecutionEnvironment.
            getExecutionEnvironment()
            .setBufferTimeout(1);

    // make parameters available in the web interface
    //env.getConfig().setGlobalJobParameters(params);
    // create input stream of integer pairs
    DataStream<Tuple2<Integer, Integer>> inputStream;
//		if (params.has("input")) {
//			inputStream = env.readTextFile(params.get("input")).map(new FibonacciInputMap());
//		} else {
    System.out.println("Executing Iterate example with default input data set.");
    System.out.println("Use --input to specify file input.");
    inputStream = env.addSource(new RandomFibonacciSource());
//		}

    // create an iterative data stream from the input with 5 second timeout
    IterativeStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> it
            = inputStream.map(new InputMap())
            .iterate(5000);

    // apply the step function to get the next Fibonacci number
    // increment the counter and split the output with the output selector
    SplitStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> step = it.
            map(new Step())
            .split(new MySelector());

    // close the iteration by selecting the tuples that were directed to the
    // 'iterate' channel in the output selector
    it.closeWith(step.select("iterate"));

    // to produce the final output select the tuples directed to the
    // 'output' channel then get the input pairs that have the greatest iteration counter
    // on a 1 second sliding window
    DataStream<Tuple2<Tuple2<Integer, Integer>, Integer>> numbers = step.select(
            "output")
            .map(new OutputMap());

    // emit results
//		if (params.has("output")) {
//			numbers.writeAsText(params.get("output"));
//		} else {
    System.out.println(
            "Printing result to stdout. Use --output to specify output path.");
    numbers.print();
//		}
//    if(params.has("flink_hdfs_output")){
//      numbers.writeAsText(params.get("flink_hdfs_output"));
//    }//hdfs://10.0.2.15:8020/Projects/projectA/Resources/test.out");

//     if(params.has("hdfs_output")){
    // execute the program
    env.execute("Streaming Iteration Example");
    
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
    if (args[1].equalsIgnoreCase("producer")) {
      Configuration hdConf = new Configuration();
      Path hdPath = new org.apache.hadoop.fs.Path(args[2]);
      FileSystem hdfs = hdPath.getFileSystem(hdConf);
      final FSDataOutputStream stream = hdfs.create(hdPath);
      stream.write("My first Flink program on Hops!".getBytes());

      HopsKafkaProducer hopsKafkaProducer = hopsKafkaUtil.
              getHopsKafkaProducer(args[0]);
      Map<String, String> message;
      for (int i = 0; i < 30; i++) {
        message = new HashMap<>();
        message.put("str1", "Henrik" + i);
        hopsKafkaProducer.produce(message);
        stream.write(("KafkaHelloWorld sending message:" + message).getBytes());
      }

     
      stream.flush();
      stream.close();
      hopsKafkaProducer.close();
    } else {
      FSDataOutputStream stream = null;

      //Consume kafka messages from topic
      HopsKafkaConsumer hopsKafkaConsumer = HopsKafkaUtil.getInstance().
              getHopsKafkaConsumer(args[0]);
      try {
        BlockingQueue<String> messages = new LinkedBlockingQueue<>();
        hopsKafkaConsumer.consume(messages);
        Configuration hdConf = new Configuration();
        Path hdPath = new org.apache.hadoop.fs.Path(args[2]);
        FileSystem hdfs = hdPath.getFileSystem(hdConf);
        stream = hdfs.create(hdPath);
        stream.write("My first Flink program on Hops!".getBytes());
        while (true) {
          String message = messages.take();
          stream = hdfs.create(hdPath);
          stream.write(("Consumer message:" + message).getBytes());
          stream.flush();
          stream.close();
        }

      } finally {
        
        hopsKafkaConsumer.close();
      }
    }
   
  }

  // *************************************************************************
  // USER FUNCTIONS
  // *************************************************************************
  /**
   * Generate BOUND number of random integer pairs from the range from 0 to
   * BOUND/2
   */
  private static class RandomFibonacciSource implements
          SourceFunction<Tuple2<Integer, Integer>> {

    private static final long serialVersionUID = 1L;

    private Random rnd = new Random();

    private volatile boolean isRunning = true;
    private int counter = 0;

    @Override
    public void run(SourceFunction.SourceContext<Tuple2<Integer, Integer>> ctx)
            throws Exception {

      while (isRunning && counter < BOUND) {
        int first = rnd.nextInt(BOUND / 2 - 1) + 1;
        int second = rnd.nextInt(BOUND / 2 - 1) + 1;

        ctx.collect(new Tuple2<>(first, second));
        counter++;
        Thread.sleep(50L);
      }
    }

    @Override
    public void cancel() {
      isRunning = false;
    }
  }

  /**
   * Generate random integer pairs from the range from 0 to BOUND/2
   */
  private static class FibonacciInputMap implements
          MapFunction<String, Tuple2<Integer, Integer>> {

    private static final long serialVersionUID = 1L;

    @Override
    public Tuple2<Integer, Integer> map(String value) throws Exception {
      String record = value.substring(1, value.length() - 1);
      String[] splitted = record.split(",");
      return new Tuple2<>(Integer.parseInt(splitted[0]), Integer.parseInt(
              splitted[1]));
    }
  }

  /**
   * Map the inputs so that the next Fibonacci numbers can be calculated while
   * preserving the original input tuple A
   * counter is attached to the tuple and incremented in every iteration step
   */
  public static class InputMap implements
          MapFunction<Tuple2<Integer, Integer>, Tuple5<Integer, Integer, Integer, Integer, Integer>> {

    private static final long serialVersionUID = 1L;

    @Override
    public Tuple5<Integer, Integer, Integer, Integer, Integer> map(
            Tuple2<Integer, Integer> value) throws
            Exception {
      return new Tuple5<>(value.f0, value.f1, value.f0, value.f1, 0);
    }
  }

  /**
   * Iteration step function that calculates the next Fibonacci number
   */
  public static class Step implements
          MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple5<Integer, Integer, Integer, Integer, Integer>> {

    private static final long serialVersionUID = 1L;

    @Override
    public Tuple5<Integer, Integer, Integer, Integer, Integer> map(
            Tuple5<Integer, Integer, Integer, Integer, Integer> value) throws
            Exception {
      return new Tuple5<>(value.f0, value.f1, value.f3, value.f2 + value.f3,
              ++value.f4);
    }
  }

  /**
   * OutputSelector testing which tuple needs to be iterated again.
   */
  public static class MySelector implements
          OutputSelector<Tuple5<Integer, Integer, Integer, Integer, Integer>> {

    private static final long serialVersionUID = 1L;

    @Override
    public Iterable<String> select(
            Tuple5<Integer, Integer, Integer, Integer, Integer> value) {
      List<String> output = new ArrayList<>();
      if (value.f2 < BOUND && value.f3 < BOUND) {
        output.add("iterate");
      } else {
        output.add("output");
      }
      return output;
    }
  }

  /**
   * Giving back the input pair and the counter
   */
  public static class OutputMap implements
          MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Integer>> {

    private static final long serialVersionUID = 1L;

    @Override
    public Tuple2<Tuple2<Integer, Integer>, Integer> map(
            Tuple5<Integer, Integer, Integer, Integer, Integer> value) throws
            Exception {
      return new Tuple2<>(new Tuple2<>(value.f0, value.f1), value.f4);
    }
  }

//  public static void main(String[] args) throws Exception {
//    String topicName;
//    String type = null;
//    int numberOfMessages = 30;
//
//    //Setup the HopsKafkaUtil
//    HopsKafkaUtil hopsKafkaUtil = HopsKafkaUtil.getInstance();
//    
//    System.out.println("SYSPROPS-ARGS:"+Arrays.toString(args)) ;
//    System.out.println("SYSPROPS-KAFKA_PROPS:"+args[args.length-1]) ;
//    
//    
//    //Check user args
//    /*
//     * USAGES: 1. topicname numberOfMessages type(producer/consumer)
//     * IF TYPE IS NOT PROVIDED, application will do both
//     * EXAMPLE: weather 30 producer
//     * weather consumer
//     */
//    Map<String, String> kafkaProps = hopsKafkaUtil.getKafkaProps(args[args.length-1]);
//    if (args != null && args.length == 4 && args[1].equals("producer")) {
//      topicName = args[0];
//      type = args[1];
//      numberOfMessages = Integer.parseInt(args[2]);
//    } else if (args != null && args.length == 3 && args[1].equals("consumer")) {
//      topicName = args[0];
//      type = args[1];
//    } else if (args != null && args.length == 2) {
//      topicName = args[0];
//    } else {
//      throw new Exception(
//              "Wrong arguments. Usage: topicName isProducer(true/false)");
//    }
//    
//    StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
//    
//    Properties sysProps = System.getProperties();
//    System.out.println("SYSPROPRS-APP:"+sysProps);
//        
//    for (Map.Entry<String, String> entry : kafkaProps.entrySet()){
//      System.out.println("KAFKA-PROP:"+entry.getKey()+","+entry.getValue());
//    }
//    //hopsKafkaUtil.setup("http://localhost:8080", "localhost");
//    hopsKafkaUtil.setup(kafkaProps.get(hopsKafkaUtil.KAFKA_SESSIONID_ENV_VAR), 
//            Integer.parseInt(kafkaProps.get(hopsKafkaUtil.KAFKA_PROJECTID_ENV_VAR)), 
//            topicName, 
//            "localhost",
//            kafkaProps.get(hopsKafkaUtil.KAFKA_BROKERADDR_ENV_VAR), 
//            "http://localhost:8080",
//            "/srv/glassfish/domain1/config/"+kafkaProps.get(hopsKafkaUtil.KAFKA_K_CERTIFICATE_ENV_VAR), 
//            "/srv/glassfish/domain1/config/"+kafkaProps.get(hopsKafkaUtil.KAFKA_T_CERTIFICATE_ENV_VAR));
//    
//		env.getConfig().disableSysoutLogging();
//		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
//
//		// very simple data generator
//		DataStream<String> messageStream = env.addSource(new SourceFunction<String>() {
//			public boolean running = true;
//
//			@Override
//			public void run(SourceContext<String> ctx) throws Exception {
//				long i = 0;
//				while(this.running && i<60) {
//					ctx.collect("Element - " + i++);
//					Thread.sleep(500);
//				}
//			}
//
//			@Override
//			public void cancel() {
//				running = false;
//			}
//		});
//    
//		// write data into Kafka
//		//messageStream.addSink(new FlinkKafkaProducer08<>(parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties()));
//    messageStream.addSink(hopsKafkaUtil.getHopsFlinkKafkaProducer(topicName,
//            new SimpleStringSchema())) ;
//    env.execute("Write into Kafka example");
//
//  }
}
