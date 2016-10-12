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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import scala.Tuple2;

import kafka.serializer.StringDecoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.SslConfigs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: JavaDirectKafkaWordCount <brokers> <topics>
 * <brokers> is a list of one or more Kafka brokers
 * <topics> is a list of one or more kafka topics to consume from
 * <p>
 * Example:
 * $ bin/run-example streaming.JavaDirectKafkaWordCount
 * broker1-host:port,broker2-host:port \
 * topic1,topic2
 */
public final class JavaDirectKafkaWordCount {

  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n"
              + "  <brokers> is a list of one or more Kafka brokers\n"
              + "  <topics> is a list of one or more kafka topics to consume from\n\n");
      System.exit(1);
    }

//    StreamingExamples.setStreamingLogLevels();
    final String topics = args[1];

    // Create context with a 2 seconds batch interval
    SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.
            seconds(2));

    Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
    final KafkaUtil util = KafkaUtil.getInstance();
    util.setup();
    // Create direct kafka stream with brokers and topics
    JavaInputDStream<ConsumerRecord<String, byte[]>> messages = util.
            createDirectStream(jssc, topicsSet);
    final String schemaStr = util.getSchema(topics);
    // Get the lines, split them into words, count the words and print
    JavaDStream<String> lines = messages.map(
            new Function<ConsumerRecord<String, byte[]>, String>() {
      @Override
      public String call(ConsumerRecord<String, byte[]> record) throws
              SchemaNotFoundException {
        try {
          Schema.Parser parser = new Schema.Parser();
          Schema schema = parser.parse(schemaStr);

          Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.
                  toBinary(schema);
          GenericRecord genericRecord = recordInjection.invert(record.value()).
                  get();

          String value = new String(record.value());
          System.out.println("record:" + record.key() + "," + value);
          System.out.println("genericRecord:" + genericRecord);
          return new String(record.value());
        } catch (Exception ex) {
          Logger.getLogger(JavaDirectKafkaWordCount.class.getName()).
                  log(Level.SEVERE, null, ex);
        }
        return "record";
      }
    });

    JavaDStream<String> words = lines.flatMap(
            new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String x) {
        System.out.println("words:" + x);
        return Arrays.asList(SPACE.split(x)).iterator();
      }
    });

    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
            new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String s) {
        System.out.println("words-s:" + s);
        return new Tuple2<>(s, 1);
      }
    }).reduceByKey(
                    new Function2<Integer, Integer, Integer>() {
              @Override
              public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
              }
            });
    wordCounts.print();

    // Start the computation
    jssc.start();
    jssc.awaitTermination();
  }
}
