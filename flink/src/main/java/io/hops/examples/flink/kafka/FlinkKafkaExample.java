package io.hops.examples.flink.kafka;

import io.hops.util.HopsConsumer;
import io.hops.util.HopsProducer;
import io.hops.util.Hops;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
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

  private static final Logger LOG = Logger.getLogger(FlinkKafkaExample.class.getName());
  private static int BOUND = 100;

  public static void main(String[] args) throws Exception {

    // obtain execution environment and set setBufferTimeout to 1 to enable
    // continuous flushing of the output buffers (lowest latency)
    StreamExecutionEnvironment env = StreamExecutionEnvironment.
        getExecutionEnvironment()
        .setBufferTimeout(1);

    // create input stream of integer pairs
    DataStream<Tuple2<Integer, Integer>> inputStream;
    LOG.info("Executing Iterate example with default input data set.");
    LOG.info("Use --input to specify file input.");
    inputStream = env.addSource(new RandomFibonacciSource());

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
    LOG.info("Printing result to stdout. Use --output to specify output path.");
    numbers.print();
    // execute the program
    env.execute("Streaming Iteration Example");

    Hops.setup(Hops.getFlinkKafkaProps(args[args.length - 1]));
    if (args[1].equalsIgnoreCase("producer")) {
      Configuration hdConf = new Configuration();
      Path hdPath = new org.apache.hadoop.fs.Path(args[2]);
      FileSystem hdfs = hdPath.getFileSystem(hdConf);
      final FSDataOutputStream stream = hdfs.create(hdPath);
      stream.write("My first Flink program on Hops!".getBytes());

      HopsProducer hopsKafkaProducer = Hops.getHopsProducer(args[0]);
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
      final HopsConsumer hopsKafkaConsumer = Hops.getHopsConsumer(args[0]);
      Thread t = new Thread() {
        @Override
        public void run() {
          hopsKafkaConsumer.consume();
        }
      };
      t.start();
      Thread.sleep(45000);
      hopsKafkaConsumer.close();

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
      MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple5<Integer, Integer, Integer, Integer, 
      Integer>> {

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

}
