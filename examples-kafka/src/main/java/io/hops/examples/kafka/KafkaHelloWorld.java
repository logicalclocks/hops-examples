package io.hops.examples.kafka;

import io.hops.kafkautil.HopsConsumer;
import io.hops.kafkautil.HopsProducer;
import io.hops.kafkautil.KafkaUtil;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Simple Kafka Producer for Hopsworks.
 * Usage: KafkaHelloWorld topicname isProducer numberOfMessages
 */
public class KafkaHelloWorld {

  public static void main(String[] args) throws Exception {
    String topicName;
    String type = null;
    int numberOfMessages = 30;
    boolean runForever = true;
    //Check user args
    /*
     * USAGES: 1. topicname numberOfMessages type(producer/consumer)
     * IF TYPE IS NOT PROVIDED, application will do both
     * EXAMPLE: weather 30 producer
     * weather consumer
     */
    if (args != null && args.length == 3 && args[1].equals("producer")) {
      topicName = args[0];
      type = args[1];
      numberOfMessages = Integer.parseInt(args[2]);
      runForever = false;
    } else if (args != null && args.length == 2 && args[1].equals("producer")) {
      topicName = args[0];
      type = args[1];
    } else if (args != null && args.length == 2 && args[1].equals("consumer")) {
      topicName = args[0];
      type = args[1];
    } else if (args != null && args.length == 1) {
      topicName = args[0];
    } else {
      throw new Exception(
              "Wrong arguments. Usage: topicName isProducer(true/false)");
    }

    //Initialize sparkcontext to be picked up by yarn, and hopsworks
    //detects the app as succeeded      
    SparkConf sparkConf = new SparkConf().setAppName("Hops Kafka Producer");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    if (type == null) {
      //Consume kafka messages from topic
      final HopsConsumer hopsKafkaConsumer = KafkaUtil.getInstance().
              getHopsConsumer(topicName);
      Thread t = new Thread() {
        public void run() {
          hopsKafkaConsumer.consume();
        }
      };
      t.start();
      //Produce Kafka messages to topic
      HopsProducer hopsKafkaProducer = KafkaUtil.getInstance().getHopsProducer(
              topicName);

      Map<String, String> message;
      for (int i = 0; i < numberOfMessages; i++) {
        message = new HashMap<>();
        message.put("platform", "HopsWorks");
        message.put("program", "SparkKafka-" + i);
        hopsKafkaProducer.produce(message);
        Thread.sleep(100);
        System.out.println("KafkaHelloWorld sending message:" + message);
      }
      Thread.sleep(8000);
      hopsKafkaProducer.close();
      hopsKafkaConsumer.close();

    } else if (type.equals("producer")) {
      //Produce Kafka messages to topic
      HopsProducer hopsKafkaProducer = KafkaUtil.getInstance().getHopsProducer(
              topicName);

      Map<String, String> message;
      int i=0;
      while(runForever || i < numberOfMessages) {
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
              topicName);
      //Keep thread alive
      //THIS WILL CAUSE THE JOB TO HANG. USER HAS TO MANUALLY STOP THE JOB.
      while (true) {
        Thread.sleep(1000);
      }
    }
    Thread.sleep(8000);
    //Stop Spark context
    jsc.stop();

  }

}
