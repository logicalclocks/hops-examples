package com.logicalclocks.flink;

import com.logicalclocks.flink.functions.AggregateRichWindowFunction;
import com.logicalclocks.flink.synk.AvroKafkaSink;
import com.logicalclocks.flink.utils.Utils;
import com.logicalclocks.flink.schemas.SourceTransaction;

import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStore;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class Main {

  private Utils utils = new Utils();

  // replace this or provide from command line arguments with your source topic name and kafka broker(s)
  private static final String BROKERS = "broker.kafka.service.consul:9091";
  private static final String SOURCE_TOPIC = "credit_card_transactions";

  public void run() throws Exception {

    // define stream env
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().enableObjectReuse();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // get hsfs handle
    FeatureStore fs = utils.getFeatureStoreHandle();

    // get feature groups
    FeatureGroup tenMinFg = fs.getFeatureGroup("card_transactions_10m_agg", 1);
    FeatureGroup oneHourFg = fs.getFeatureGroup("card_transactions_10m_agg", 1);
    FeatureGroup twelveHFg = fs.getFeatureGroup("card_transactions_10m_agg", 1);

    // get source stream
    DataStream<SourceTransaction> sourceStream = utils.getSourceKafkaStream(env, BROKERS, SOURCE_TOPIC);

    // compute 10 min aggregations
    DataStream<byte[]> tenMinRecord =
        sourceStream.keyBy(SourceTransaction::getCcNumber)
            .window(TumblingEventTimeWindows.of(Time.minutes(10)))
            .apply(new AggregateRichWindowFunction(tenMinFg.getDeserializedAvroSchema(),
                tenMinFg.getFeatures().stream().map(Feature::getName).collect(Collectors.toList())));

    //send to online fg topic
    List<String> tenMinFgPk = tenMinFg.getFeatures().stream().filter(Feature::getPrimary)
        .map(Feature::getName).collect(Collectors.toList());
    Properties tenMinKafkaProps = utils.getKafkaProperties(tenMinFg);
    tenMinRecord.addSink(new FlinkKafkaProducer<byte[]>(tenMinFg.getOnlineTopicName(),
        new AvroKafkaSink(String.join(",", tenMinFgPk), tenMinFg.getOnlineTopicName()),
        tenMinKafkaProps,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));

    // compute 1 hour aggregations
    DataStream<byte[]> oneHourRecord =
        sourceStream.keyBy(SourceTransaction::getCcNumber)
            .window(TumblingEventTimeWindows.of(Time.minutes(60)))
            .apply(new AggregateRichWindowFunction(oneHourFg.getDeserializedAvroSchema(),
                oneHourFg.getFeatures().stream().map(Feature::getName).collect(Collectors.toList())));

    //send to online fg topic
    List<String> oneHourFgPk = oneHourFg.getFeatures().stream().filter(Feature::getPrimary)
        .map(Feature::getName).collect(Collectors.toList());
    Properties oneHourKafkaProps = utils.getKafkaProperties(oneHourFg);
    oneHourRecord.addSink(new FlinkKafkaProducer<byte[]>(oneHourFg.getOnlineTopicName(),
        new AvroKafkaSink(String.join(",", oneHourFgPk), oneHourFg.getOnlineTopicName()),
        oneHourKafkaProps, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));

    // compute 12 hour aggregations
    DataStream<byte[]> twelveHRecord =
        sourceStream.keyBy(SourceTransaction::getCcNumber)
            .window(TumblingEventTimeWindows.of(Time.minutes(60 * 12)))
            .apply(new AggregateRichWindowFunction(twelveHFg.getDeserializedAvroSchema(),
                twelveHFg.getFeatures().stream().map(Feature::getName).collect(Collectors.toList())));

    //send to online fg topic
    List<String> twelveHFgPk = twelveHFg.getFeatures().stream().filter(Feature::getPrimary)
        .map(Feature::getName).collect(Collectors.toList());
    Properties twelveHKafkaProps = utils.getKafkaProperties(twelveHFg);
    twelveHRecord.addSink(new FlinkKafkaProducer<byte[]>(twelveHFg.getOnlineTopicName(),
        new AvroKafkaSink(String.join(",", twelveHFgPk), twelveHFg.getOnlineTopicName()),
        twelveHKafkaProps, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));

    env.execute();
  }

  public static void main(String[] args) throws Exception {
    Main demo = new Main();
    demo.run();
  }
}
