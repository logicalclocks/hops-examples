package io.hops.examples.spark.kafka;

import io.hops.util.HopsConsumer;
import io.hops.util.HopsProducer;
import io.hops.util.HopsUtil;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.common.Cluster;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Simple Kafka Producer for Hopsworks.
 * Usage: SimpleKafkaProcess <topic> <type(producer|consumer)> <messages>
 * <topic> Name of the Kafka topic.
 * <type> Type of the KAfka process, producer or consumer.
 * <messages> (Optional) Number of messages to produce. If not provided,
 * producer
 * will send messages infinitely.
 * Example:
 * SimpleKafkaProcess mytopic producer 50
 */
public class SimpleKafkaProcess {
  
  private static final Logger LOG = Logger.getLogger(SimpleKafkaProcess.class.getName());
  
  public static void main(String[] args) throws Exception {
    String topic = null;
    String type = null;
    int numberOfMessages = 30;
    boolean runForever = true;
    //Check user args
    if (args != null && args.length == 3 && args[1].equals("producer")) {
      topic = args[0];
      type = args[1];
      numberOfMessages = Integer.parseInt(args[2]);
      runForever = false;
    } else if (args != null && args.length == 2 && args[1].equals("producer")) {
      topic = args[0];
      type = args[1];
    } else if (args != null && args.length == 2 && args[1].equals("consumer")) {
      topic = args[0];
      type = args[1];
    } else {
      LOG.log(Level.SEVERE, "Wrong arguments. Usage: topicName isProducer(true/false)");
      System.exit(1);
    }

    //Initialize sparkcontext to be picked up by yarn, and hopsworks
    //detects the app as succeeded      
    SparkConf sparkConf = new SparkConf().setAppName("Hops Kafka Producer");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    if (type.equals("producer")) {
      List<InetSocketAddress> brokers = new ArrayList<>();
      InetSocketAddress broker = new InetSocketAddress("10.0.2.15", 9091);
      brokers.add(broker);
      Cluster cluster = Cluster.bootstrap(brokers);
      LOG.log(Level.INFO, "Topics:{0}", cluster.topics());
      LOG.log(Level.INFO, "Get availablePartitionsForTopic:{0}, {1}", new Object[]{topic,
        cluster.availablePartitionsForTopic(topic)});
      LOG.log(Level.INFO, "Get availablePartitionsForTopic:{0}, {1}", new Object[]{topic,
        cluster.availablePartitionsForTopic(
            "yourtopic")});

      //Produce Kafka messages to topic
      HopsProducer hopsKafkaProducer = HopsUtil.getHopsProducer(topic);

      Map<String, String> message;
      int i = 0;
      while (runForever || i < numberOfMessages) {
        message = new HashMap<>();
        message.put("platform", "HopsWorks");
        message.put("program", "SparkKafka-" + i);
        hopsKafkaProducer.produce(message);
        Thread.sleep(100);
        i++;
        LOG.log(Level.INFO, "KafkaHelloWorld sending message:{0}", message);

      }
      hopsKafkaProducer.close();
    } else {
      //Consume kafka messages from topic
      HopsConsumer hopsKafkaConsumer = HopsUtil.getHopsConsumer(topic);
      //Keep thread alive
      //THIS WILL CAUSE THE JOB TO HANG. USER HAS TO MANUALLY STOP THE JOB.
      while (true) {
        Thread.sleep(1000);
      }
    }
    Thread.sleep(5000);
    //Stop Spark context
    jsc.stop();

  }

}
