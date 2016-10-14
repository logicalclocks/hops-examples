package io.hops.examples.spark;

import io.hops.kafkautil.HopsConsumer;
import io.hops.kafkautil.HopsProducer;
import io.hops.kafkautil.KafkaUtil;
import java.util.HashMap;
import java.util.Map;
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
      System.err.println(
              "Wrong arguments. Usage: topicName isProducer(true/false)");
      System.exit(1);
    }

    //Initialize sparkcontext to be picked up by yarn, and hopsworks
    //detects the app as succeeded      
    SparkConf sparkConf = new SparkConf().setAppName("Hops Kafka Producer");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    if (type.equals("producer")) {
      //Produce Kafka messages to topic
      HopsProducer hopsKafkaProducer = KafkaUtil.getInstance().getHopsProducer(
              topic);

      Map<String, String> message;
      int i = 0;
      while (runForever || i < numberOfMessages) {
        message = new HashMap<>();
        message.put("platform", "HopsWorks");
        message.put("program", "SparkKafka-" + i);
        hopsKafkaProducer.produce(message);
        Thread.sleep(100);
        i++;
        System.out.println("KafkaHelloWorld sending message:" + message);

      }
      hopsKafkaProducer.close();
    } else {
      //Consume kafka messages from topic
      HopsConsumer hopsKafkaConsumer = KafkaUtil.getInstance().getHopsConsumer(
              topic);
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
