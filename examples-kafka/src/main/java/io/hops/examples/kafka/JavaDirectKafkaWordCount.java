/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.examples.kafka;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import io.hops.kafkautil.KafkaUtil;
import io.hops.kafkautil.SchemaNotFoundException;
import io.hops.kafkautil.spark.SparkConsumer;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Pattern;

import scala.Tuple2;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: JavaDirectKafkaWordCount <brokers> <topics>
 * <brokers> is a list of one or more Kafka brokers
 * <topics> is a list of one or more kafka topics to consume from
 * <p>
 * Example:
 * $ bin/run-example streaming.JavaDirectKafkaWordCount
 * topic1,topic2
 * <p>
 */
public final class JavaDirectKafkaWordCount {

  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(final String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: JavaDirectKafkaWordCount <topics> <sink>\n"
              + "  <topics> is a list of one or more kafka topics to consume from\n"
              + "  <sink> location in hdfs to append streaming output om\n\n");
      System.exit(1);
    }

    final String topics = args[0];

    // Create context with a 2 seconds batch interval
    SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.
            seconds(2));
    Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
    //Get HopsWorks Kafka Utility instance
    KafkaUtil kafkaUtil = KafkaUtil.getInstance();

    SparkConsumer consumer = kafkaUtil.getSparkConsumer(jssc, topicsSet);
    // Create direct kafka stream with topics
    JavaInputDStream<ConsumerRecord<String, byte[]>> messages = consumer.
            createDirectStream();
    
    //Get the schema for which to consume messages
    final String avroSchema = kafkaUtil.getSchema(topics);
    final StringBuilder line = new StringBuilder();
    
    
    // Get the lines, split them into words, count the words and print
    JavaDStream<String> lines = messages.map(
            new Function<ConsumerRecord<String, byte[]>, String>() {
      @Override
      public String call(ConsumerRecord<String, byte[]> record) throws
              SchemaNotFoundException {
        line.setLength(0);
        //Parse schema and generate Avro record
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(avroSchema);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.
                toBinary(schema);
        GenericRecord genericRecord = recordInjection.invert(record.value()).
                get();

        line.append(((Utf8) genericRecord.get("platform")).toString()).
                append(" ").
                append(((Utf8) genericRecord.get("program")).toString());
        return line.toString();
      }
    });

    JavaDStream<String> words = lines.flatMap(
            new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String x) {
        return Arrays.asList(SPACE.split(x)).iterator();
      }
    });

    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
            new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String s) {
        return new Tuple2<>(s, 1);
      }
    }).reduceByKey(new Function2<Integer, Integer, Integer>() {
              @Override
              public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
              }
            });

    wordCounts.print();

    /*
     * Based on Spark Design patterns
     * http://spark.apache.org/docs/latest/streaming-programming-guide.html#output-operations-on-dstreams
     */
    wordCounts.foreachRDD(
            new VoidFunction2<JavaPairRDD<String, Integer>, Time>() {
      @Override
      public void call(JavaPairRDD<String, Integer> rdd, Time time) throws
              Exception {
        //Keep the latest microbatch output in the file
        rdd.repartition(1).saveAsHadoopFile(args[1], String.class, String.class,
                TextOutputFormat.class);
      }

    });

    /*
     * Enable this to all the streaming outputs. It creates a folder for every
     * microbatch slot
     */
//    wordCounts.saveAsHadoopFiles(args[1], "txt", String.class, String.class,
//                    (Class) TextOutputFormat.class);
    // Start the computation
    jssc.start();
    jssc.awaitTermination();
  }
}
