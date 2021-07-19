package com.logicalclocks.flink.utils;

import com.logicalclocks.flink.schemas.SourceTransaction;
import com.logicalclocks.flink.schemas.SourceTransactionSchema;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.HopsworksConnection;
import com.logicalclocks.hsfs.metadata.KafkaApi;

import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.stream.Collectors;

public class Utils {

  private KafkaApi kafkaApi = new KafkaApi();

  public Properties getKafkaProperties(FeatureGroup featureGroup) throws Exception {
    Properties dataKafkaProps = new Properties();
    String materialPasswd = readMaterialPassword();
    dataKafkaProps.setProperty("bootstrap.servers",
        kafkaApi.getBrokerEndpoints(featureGroup.getFeatureStore()).stream().map(broker -> broker.replaceAll(
        "INTERNAL://", "")).collect(Collectors.joining(",")));
    // These settings are static and they don't need to be changed
    dataKafkaProps.setProperty("security.protocol", "SSL");
    dataKafkaProps.setProperty("ssl.truststore.location", "t_certificate");
    dataKafkaProps.setProperty("ssl.truststore.password", materialPasswd);
    dataKafkaProps.setProperty("ssl.keystore.location", "k_certificate");
    dataKafkaProps.setProperty("ssl.keystore.password", materialPasswd);
    dataKafkaProps.setProperty("ssl.key.password", materialPasswd);
    dataKafkaProps.setProperty("ssl.endpoint.identification.algorithm", "");

    return dataKafkaProps;
  }

  public Properties getKafkaProperties(String broker) throws Exception {
    Properties dataKafkaProps = new Properties();
    String materialPasswd = readMaterialPassword();
    dataKafkaProps.setProperty("bootstrap.servers", broker);
    // These settings are static and they don't need to be changed
    dataKafkaProps.setProperty("security.protocol", "SSL");
    dataKafkaProps.setProperty("ssl.truststore.location", "t_certificate");
    dataKafkaProps.setProperty("ssl.truststore.password", materialPasswd);
    dataKafkaProps.setProperty("ssl.keystore.location", "k_certificate");
    dataKafkaProps.setProperty("ssl.keystore.password", materialPasswd);
    dataKafkaProps.setProperty("ssl.key.password", materialPasswd);
    dataKafkaProps.setProperty("ssl.endpoint.identification.algorithm", "");

    return dataKafkaProps;
  }

  /**
   * Setup the Kafka source stream.
   *
   * The Kafka topic is populated by the same producer notebook.
   * The stream at this stage contains just string.
   *
   * @param env The Stream execution environment to which add the source
   * @param brokers the list of brokers to use as bootstrap servers for Kafka
   * @param sourceTopic the Kafka topic to read the data from
   * @return the DataStream object
   * @throws Exception
   */

  public DataStream<SourceTransaction> getSourceKafkaStream(StreamExecutionEnvironment env,
                                                            String brokers, String sourceTopic) throws Exception {

    FlinkKafkaConsumerBase<SourceTransaction> kafkaSource = new FlinkKafkaConsumer<>(
        sourceTopic, new SourceTransactionSchema(), getKafkaProperties(brokers)).setStartFromEarliest();
    kafkaSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SourceTransaction>() {
      @Override
      public long extractAscendingTimestamp(SourceTransaction element) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Long timeStamp = null;
        try {
          timeStamp = dateFormat.parse(element.getDatetime()).getTime();
        } catch (ParseException e) {
          e.printStackTrace();
        }
        return timeStamp;
      }
    });
    return env.addSource(kafkaSource);
  }

  public FeatureStore getFeatureStoreHandle() throws IOException, FeatureStoreException {
    // establish connection to feature store
    // set necessary variables. this is temporary solution until flink is fully integrated with hsfs
    System.setProperty("hopsworks.restendpoint", "https://hopsworks.glassfish.service.consul:8182");
    System.setProperty("hopsworks.domain.truststore", "t_certificate");

    //get handle
    HopsworksConnection connection = HopsworksConnection.builder().build();
    return connection.getFeatureStore();
  }

  private static String readMaterialPassword() throws Exception {
    return FileUtils.readFileToString(new File("material_passwd"));
  }
}
