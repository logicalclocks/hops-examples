package io.hops.examples.kafka;

import io.hops.kafka.HopsKafkaConsumer;
import io.hops.kafka.HopsKafkaProducer;
import io.hops.kafka.HopsKafkaUtil;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * SparkPi for HopsKafka.
 * Usage: KafkaMain topicname isProducer numberOfMessages
 */
public class KafkaMain {

    public static void main(String[] args) throws Exception {
        String topicName = "weather";
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
        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkPi");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
         int slices = 1;// (args.length == 1) ? Integer.parseInt(args[0]) : 2;
        int n = 100000 * slices;
        List<Integer> l = new ArrayList<Integer>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

        int count = dataSet.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) {
                double x = Math.random() * 2 - 1;
                double y = Math.random() * 2 - 1;
                return (x * x + y * y < 1) ? 1 : 0;
            }
        }).reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) {
                return integer + integer2;
            }
        });

        //System.out.println("Pi is roughly " + 4.0 * count / n);

        
        jsc.stop();
        
    }

}
