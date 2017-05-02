package io.hops.examples.spark.kafka;

import io.hops.util.HopsUtil;
import io.hops.util.SchemaNotFoundException;
import io.hops.util.spark.SparkConsumer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Time;
import org.json.JSONObject;

/**
 * Consumes log messages from one or more topics in Kafka, generates a json to elasticsearch index and archives data
 * in Parquet format in HDFS.
 * <p>
 * Usage: StreamingKafkaElastic <sink>
 * <sink> location in hdfs to append streaming output
 * <p>
 * Example: /Projects/MyProject/Sink/Data
 * <p>
 */
public final class StreamingKafkaElastic {

  private static final Logger LOG = Logger.getLogger(StreamingKafkaElastic.class.getName());

  public static void main(final String[] args) throws Exception {

    SparkConf sparkConf = new SparkConf().setAppName(HopsUtil.getJobName());
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

    //Use applicationId for sink folder
    final String appId = jssc.sparkContext().getConf().getAppId();
    SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
    //Get consumer groups
    Properties props = new Properties();
    props.put("value.deserializer", StringDeserializer.class.getName());
    props.put("client.id", HopsUtil.getJobName());
    SparkConsumer consumer = HopsUtil.getSparkConsumer(jssc, props);
    //Store processed offsets

    // Create direct kafka stream with topics
    JavaInputDStream<ConsumerRecord<String, String>> messages = consumer.createDirectStream();

    //Convert line to JSON
    JavaDStream<LogEntry> logEntries = messages.map(new Function<ConsumerRecord<String, String>, JSONObject>() {
      @Override
      public JSONObject call(ConsumerRecord<String, String> record) throws SchemaNotFoundException,
          MalformedURLException, ProtocolException {
        LOG.info("record:" + record);

        return parser(args[1], record.value(), appId);
      }
    }).map(new Function<JSONObject, LogEntry>() {
      @Override
      public LogEntry call(JSONObject json) throws SchemaNotFoundException, MalformedURLException, ProtocolException,
          IOException {
        LogEntry logEntry
            = new LogEntry(json.getString("message").replace("\n\t", "\n").replace("\n", "---"), json.getString("priority"), json.getString("logger_name"), json.
                getString("thread"), json.getString("timestamp"));
        LOG.info("LogEntry:" + logEntry);
        return logEntry;
      }
    });

    //logEntries.print();
    logEntries.repartition(1).foreachRDD(new VoidFunction2<JavaRDD<LogEntry>, Time>() {
      @Override
      public void call(JavaRDD<LogEntry> rdd, Time time) throws
          Exception {
        Dataset<Row> row = sparkSession.createDataFrame(rdd, LogEntry.class);
        row.write().mode(SaveMode.Append).parquet("/Projects/" + HopsUtil.getProjectName() + "/Resources/Parquet");
      }
    });
    /*
     * Enable this to get all the streaming outputs. It creates a folder for
     * every microbatch slot.
     * ///////////////////////////////////////////////////////////////////////
     * wordCounts.saveAsHadoopFiles(args[1], "txt", String.class,
     * String.class, (Class) TextOutputFormat.class);
     * ///////////////////////////////////////////////////////////////////////
     */
    // Start the computation
    jssc.start();
    HopsUtil.shutdownGracefully(jssc);
  }

  private static JSONObject parser(String logType, String line, String appId) {
    JSONObject jsonLog = new JSONObject(line);
    JSONObject index = new JSONObject();
    String priority, logger, thread, timestamp;
    priority = logger = thread = timestamp = null;
    if (logType.equalsIgnoreCase("appserver")) {
      //Catch an error getting log attributes and set default ones in this case
      String[] attrs = jsonLog.getString("message").split("\\|");
      String message;
      try {
        message = attrs[6];
        priority = attrs[2];
        logger = attrs[3];
        thread = attrs[5];
        timestamp = attrs[1];
        //Convert timestamp to appropriate format
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSSZ");
        Date result = df.parse(timestamp);
        Locale currentLocale = Locale.getDefault();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", currentLocale);
        timestamp = format.format(result);

      } catch (Exception ex) {
        LOG.warning("Error while parsing log, setting default index parameters:" + ex.getMessage());
        message = jsonLog.getString("message");
        priority = "parse error";
        logger = "parse error";
        thread = "parse error";
        timestamp = "parse error";
      }

      index.put("@version", "1");
      index.put("@timestamp", jsonLog.getString("@timestamp"));
      index.put("message", message);
      index.put("timestamp", timestamp);
      index.put("path", "?");
      index.put("priority", priority);
      index.put("logger_name", logger);
      index.put("thread", thread);
      index.put("class", "?");
      index.put("file", jsonLog.getString("source"));
      index.put("application", appId);
      index.put("host", jsonLog.getJSONObject("beat").getString("hostname"));
      index.put("project", HopsUtil.getProjectName());
      index.put("method", "?");
      index.put("jobname", HopsUtil.getJobName());
    } else if (logType.equalsIgnoreCase("custom")) {
      //Catch an error getting log attributes and set default ones in this case
      //Example message
      //2017-01-19 14:24:40.693 [main] INFO  class.myclass : Creating connector (http) on port 8080
      String[] attrs = null;
      String[] messageAttrs = null;
      String message;
      String log = jsonLog.getString("message");
      thread = log.substring(log.indexOf("[") + 1, log.indexOf("]"));
      log = log.substring(0,log.indexOf("[")) + log.substring(log.indexOf("]")+1);
      try {
        if (log.contains(" : \n")) {
          attrs = log.split(" : \n")[0].split("\\s+");
          messageAttrs = log.split(" : \n");
        } else {
          attrs = log.split(" : ")[0].split("\\s+");
          messageAttrs = log.split(" : ");
        }

        StringBuilder messageBuilder = new StringBuilder();
        for (int i = 1; i < messageAttrs.length; i++) {
          messageBuilder.append(messageAttrs[i]);
        }
        message = messageBuilder.toString();
        priority = attrs[2];
        logger = attrs[3];

        timestamp = attrs[0] + " " + attrs[1];
        //Convert timestamp to appropriate format
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        Date result = df.parse(timestamp);
        Locale currentLocale = Locale.getDefault();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", currentLocale);
        timestamp = format.format(result);
      } catch (Exception ex) {
        LOG.warning("Error while parsing log, setting default index parameters:" + ex.getMessage());
        message = jsonLog.getString("message");
        priority = "parse error";
        logger = "parse error";
        thread = "parse error";
        timestamp = "parse error";
      }

      index.put("@version", "1");
      index.put("@timestamp", jsonLog.getString("@timestamp"));
      index.put("message", message);
      index.put("timestamp", timestamp);
      index.put("path", "?");
      index.put("priority", priority);
      index.put("logger_name", logger);
      index.put("thread", thread);
      index.put("class", "?");
      index.put("file", jsonLog.getString("source"));
      index.put("application", appId);
      index.put("host", jsonLog.getJSONObject("beat").getString("hostname"));
      index.put("project", HopsUtil.getProjectName());
      index.put("method", "?");
      index.put("jobname", HopsUtil.getJobName());
    }
    //LOG.log(Level.INFO, "hops index:{0}", index.toString());
    URL obj;
    HttpURLConnection conn = null;
    BufferedReader br = null;
    try {
//      LOG.log(Level.INFO, "elastic url:" + "http://10.0.2.15:9200/" + HopsUtil.getProjectName() + "/logs");
      obj = new URL("http://"+HopsUtil.getElasticEndPoint()+"/" + HopsUtil.getProjectName() + "/logs");
      conn = (HttpURLConnection) obj.openConnection();
      conn.setDoOutput(true);
      conn.setRequestMethod("POST");
      try (OutputStreamWriter out = new OutputStreamWriter(conn.getOutputStream())) {
        out.write(index.toString());
      }
      br = new BufferedReader(new InputStreamReader(
          (conn.getInputStream())));

      String output;
      StringBuilder outputBuilder = new StringBuilder();
      while ((output = br.readLine()) != null) {
        outputBuilder.append(output);
      }
//      LOG.log(Level.INFO, "output:{0}", output);

    } catch (IOException ex) {
      LOG.log(Level.SEVERE, ex.toString(), ex);
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
      obj = null;
      if (br != null) {
        try {
          br.close();
        } catch (IOException ex) {
          LOG.log(Level.SEVERE, ex.toString(), ex);
        }
      }
    }

    return index;
  }

}
