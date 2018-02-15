package io.hops.examples.spark.kafka;

import com.google.common.base.Strings;
import io.hops.util.exceptions.CredentialsNotFoundException;
import io.hops.util.HopsUtil;
import io.hops.util.exceptions.WorkflowManagerException;
import io.hops.util.spark.SparkConsumer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
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
    JavaDStream<LogEntryFilebeat> logEntries = messages.map(new Function<ConsumerRecord<String, String>, JSONObject>() {
      @Override
      public JSONObject call(ConsumerRecord<String, String> record) throws Exception {
        return parser(args[0], record.value(), appId);
      }
    }).map(new Function<JSONObject, LogEntryFilebeat>() {
      @Override
      public LogEntryFilebeat call(JSONObject json) throws Exception {
        LogEntryFilebeat logEntry
            = new LogEntryFilebeat(json.getString("message").replace("\n\t", "\n").replace("\n", "---"), json.getString(
                "priority"), json.getString("logger_name"), json.
                getString("thread"), json.getString("timestamp"), json.getString("file"));
        return logEntry;
      }
    });

    logEntries.repartition(1).foreachRDD(new VoidFunction2<JavaRDD<LogEntryFilebeat>, Time>() {
      @Override
      public void call(JavaRDD<LogEntryFilebeat> rdd, Time time) throws Exception {
        Dataset<Row> row = sparkSession.createDataFrame(rdd, LogEntryFilebeat.class);
        String dataset = "Resources";
        if (!rdd.isEmpty()) {
          LOG.log(Level.INFO, "hops rdd:{0}", rdd.first().getFile());
          if (rdd.first().getFile().contains("fiona")) {
            dataset = "Fiona";
          } else if (rdd.first().getFile().contains("shrek")) {
            dataset = "Shrek";
          }

          DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd");
          LocalDate localDate = LocalDate.now();
          row.write().mode(SaveMode.Append).parquet("/Projects/" + HopsUtil.getProjectName() + "/" + dataset + "/Logs-"
              + dtf.format(localDate));
        }
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

  private static JSONObject parser(String logType, String line, String appId)
      throws CredentialsNotFoundException, WorkflowManagerException {
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
        LOG.log(Level.WARNING, "Error while parsing log, setting default index parameters:{0}", ex.getMessage());
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
      log = log.substring(0, log.indexOf("[")) + log.substring(log.indexOf("]") + 1);
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
        LOG.log(Level.WARNING, "Error while parsing log, setting default index parameters:{0}", ex.getMessage());
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
      if (jsonLog.getString("source").contains("/")) {
        index.put("file", jsonLog.getString("source").substring(jsonLog.getString("source").lastIndexOf("/") + 1));
      } else {
        index.put("file", jsonLog.getString("source"));
      }
      index.put("application", appId);
      index.put("host", jsonLog.getJSONObject("beat").getString("hostname"));
      index.put("project", HopsUtil.getProjectName());
      index.put("method", "?");
      index.put("jobname", HopsUtil.getJobName());

      if (jsonLog.getString("source").contains("shrek")) {
        index.put("location", "59.32,18.06");
      } else {
        index.put("location", "40.71,-74.00");
      }
      if (!Strings.isNullOrEmpty(priority) && priority.equalsIgnoreCase("TRACE")) {
        LOG.log(Level.INFO, "Sending email");
        HopsUtil.getWorkflowManager().sendEmail("tkak@kth.se", "Error message received", timestamp + " :: " + message);
      }

    }
    URL obj;
    HttpURLConnection conn = null;
    BufferedReader br = null;
    try {
      obj = new URL("http://" + HopsUtil.getElasticEndPoint() + "/" + HopsUtil.getProjectName().toLowerCase()
          + "/logs");
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
