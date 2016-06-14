package io.hops.examples.kafka;

import io.hops.kafka.HopsKafkaConsumer;
import io.hops.kafka.HopsKafkaProducer;
import io.hops.kafka.HopsKafkaUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * SparkPi for HopsKafka.
 * Usage: KafkaHelloWorld topicname isProducer numberOfMessages
 */
public class KafkaHelloWorld {

    public static void main(String[] args) throws Exception {
        String topicName;
        String type = null;
        int numberOfMessages = 30;
              
        //Check user args
        /*
        USAGES: 1. topicname  numberOfMessages type(producer/consumer)
                IF TYPE IS NOT PROVIDED, application will do both
        EXAMPLE: weather 30 producer
                 weather consumer
        */
      
        if(args != null && args.length == 3){
            topicName = args[0];
            numberOfMessages = Integer.parseInt(args[1]);
            type = args[2];
        } else if(args != null && args.length == 1){
            topicName = args[0];
        } else {
            throw new Exception("Wrong arguments. Usage: topicName isProducer(true/false)");
        }
      

        //Setup the HopsKafkaUtil
        HopsKafkaUtil hopsKafkaUtil = HopsKafkaUtil.getInstance();
        hopsKafkaUtil.setup(topicName);
        
        if(type == null){
            //Consume kafka messages from topic
            HopsKafkaConsumer hopsKafkaConsumer = new HopsKafkaConsumer(topicName);
            hopsKafkaConsumer.start();
             //Produce Kafka messages to topic
            HopsKafkaProducer hopsKafkaProducer = new HopsKafkaProducer(topicName, false, numberOfMessages);
            hopsKafkaProducer.run();
        } else {
            if(type.equals("producer")){
               //Produce Kafka messages to topic
                HopsKafkaProducer hopsKafkaProducer = new HopsKafkaProducer(topicName, false, numberOfMessages);
                hopsKafkaProducer.run();
            } else {
                //Consume kafka messages from topic
                HopsKafkaConsumer hopsKafkaConsumer = new HopsKafkaConsumer(topicName);
                hopsKafkaConsumer.start();
                //Keep thread alive
                //THIS WILL CAUSE THE JOB TO HANG. CHANGE IT.
                while(true){
                    Thread.sleep(1000);
                }
            } 
        }

         //Initialize sparkcontext to be picked up by yarn, and hopsworks
        //detects the app as succeeded      
        SparkConf sparkConf = new SparkConf().setAppName("Kafka");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        jsc.stop();
        
    }

}
