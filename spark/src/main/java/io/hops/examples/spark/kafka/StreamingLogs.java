package io.hops.examples.spark.kafka;

import io.hops.util.HopsUtil;
import io.hops.util.SchemaNotFoundException;
import io.hops.util.spark.SparkConsumer;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.StringUtils;

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
public final class StreamingLogs {

  private static final Logger LOG = Logger.getLogger(StreamingLogs.class.getName());

  public static void main(final String[] args) throws Exception {

    SparkConf sparkConf = new SparkConf().setAppName(HopsUtil.getJobName());
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

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
    JavaDStream<NamenodeLogEntry> logEntries = messages.map(new Function<ConsumerRecord<String, String>, JSONObject>() {
      @Override
      public JSONObject call(ConsumerRecord<String, String> record) throws SchemaNotFoundException,
          MalformedURLException, ProtocolException {
        LOG.log(Level.INFO, "record:{0}", record);
        return parser(record.value(), appId);
      }
    }).map(new Function<JSONObject, NamenodeLogEntry>() {
      @Override
      public NamenodeLogEntry call(JSONObject json) throws SchemaNotFoundException, MalformedURLException,
          ProtocolException,
          IOException {
        NamenodeLogEntry logEntry
            = new NamenodeLogEntry(json.getString("message").replace("\n\t", "\n").replace("\n", "---"),
                json.getString("priority"),
                json.getString("logger_name"),
                json.getString("timestamp"),
                json.getString("file"));
        LOG.log(Level.INFO, "NamenodeLogEntry:{0}", logEntry);
        return logEntry;
      }
    });

    //logEntries.print();
    logEntries.foreachRDD(new VoidFunction2<JavaRDD<NamenodeLogEntry>, Time>() {
      @Override
      public void call(JavaRDD<NamenodeLogEntry> rdd, Time time) throws
          Exception {
        Dataset<Row> row = sparkSession.createDataFrame(rdd, NamenodeLogEntry.class);
        if (!rdd.isEmpty()) {
          row.write().mode(SaveMode.Append).
              parquet("/Projects/" + HopsUtil.getProjectName() + "/Resources/LogAnalysis");
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

  private static JSONObject parser(String line, String appId) {
    JSONObject jsonLog = new JSONObject(line);
    JSONObject index = new JSONObject();
    String priority, logger, thread, timestamp;
    priority = logger = thread = timestamp = null;

    //Sample line:
    String[] attrs = jsonLog.getString("message").substring(0, StringUtils.ordinalIndexOf(jsonLog.getString("message"),
        " ", 4)).split(" ");
    String message = jsonLog.getString("message").substring(StringUtils.
        ordinalIndexOf(jsonLog.getString("message"), " ", 4) + 1);
    try {
      priority = attrs[2];
      logger = attrs[3];
      //thread = attrs[5];
      timestamp = attrs[0] + " " + attrs[1];
      //Convert timestamp to appropriate format
      DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");
      Date result = df.parse(timestamp);
      Locale currentLocale = Locale.getDefault();
      SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", currentLocale);
      timestamp = format.format(result);

    } catch (Exception ex) {
      LOG.log(Level.WARNING, "Error while parsing log, setting default index parameters:{0}", ex.getMessage());
      message = jsonLog.getString("message");
      priority = "parse error";
      logger = "parse error";
      //thread = "parse error";
      timestamp = "parse error";
    }

    index.put("message", message);
    index.put("priority", priority);
    index.put("logger_name", logger);
    index.put("timestamp", timestamp);
    index.put("application", appId);
    index.put("host", jsonLog.getJSONObject("beat").getString("hostname"));
    index.put("project", HopsUtil.getProjectName());
    index.put("jobname", HopsUtil.getJobName());
    if (jsonLog.getString("source").contains("/")) {
      index.put("file", jsonLog.getString("source").substring(jsonLog.getString("source").lastIndexOf("/") + 1));
    } else {
      index.put("file", jsonLog.getString("source"));
    }

    return index;
  }

}
