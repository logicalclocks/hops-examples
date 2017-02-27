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
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.json.JSONObject;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount,
 * produces
 * hello world messages to Kafka using Hops Kafka Producer. Streaming code based
 * on Spark JavaDirectKafkaWordCount.
 * Usage: StreamingExample <type> <sink>
 * <type> type of kafka process (producer|consumer)
 * <sink> location in hdfs to append streaming output
 * <p>
 * Example:
 * $ bin/run-example streaming.StreamingExample
 * consumer /Projects/MyProject/Sink/Data
 * <p>
 */
public final class StreamingKafkaElastic {

  private static Logger LOGGER = Logger.getLogger(StreamingKafkaElastic.class.getName());
  private static final Pattern NEWLINE = Pattern.compile("\n");

  public static void main(final String[] args) throws Exception {

    SparkConf sparkConf = new SparkConf().setAppName("StreamingExample");

    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
        Durations.seconds(10));

    SparkSession session = SparkSession.builder().config(sparkConf).getOrCreate();
    //Use applicationId for sink folder
    final String appId = jssc.sparkContext().getConf().getAppId();

    //Get consumer groups
    Properties props = new Properties();
    props.put("value.deserializer", StringDeserializer.class.getName());
    SparkConsumer consumer = HopsUtil.getSparkConsumer(jssc, props);
    // Create direct kafka stream with topics
    JavaInputDStream<ConsumerRecord<String, String>> messages = consumer.
        createDirectStream();

    // Get the lines, split them into words, count the words and print
    JavaDStream<String> records = messages.map(
        new Function<ConsumerRecord<String, String>, String>() {
      @Override
      public String call(ConsumerRecord<String, String> record) throws SchemaNotFoundException {
        return record.value();
      }
    });
    //records.print();

    JavaDStream<String> lines = records.flatMap(
        new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String x) {
        return Arrays.asList(NEWLINE.split(x)).iterator();
      }
    });

    //Save hdfs file
    lines.foreachRDD(
        new VoidFunction2<JavaRDD<String>, Time>() {
      @Override
      public void call(JavaRDD<String> rdd, Time time) throws
          Exception {
        //Keep the latest microbatch output in the file
        rdd.saveAsTextFile(args[0] + "-" + appId);
      }
    });

    //lines.print();
    //Convert line to JSON
    JavaDStream<JSONObject> jsons = lines.map(
        new Function<String, JSONObject>() {
      @Override
      public JSONObject call(String line) throws SchemaNotFoundException, MalformedURLException, ProtocolException,
          IOException {
        JSONObject jsonLog = new JSONObject(line);
        JSONObject index = new JSONObject();
        String message, priority, logger, thread, timestamp;
        message = priority = logger = thread = timestamp = null;
        //Catch an error getting log attributes and set default ones in this case
        String[] attrs = jsonLog.getString("message").split("\\|");
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

        } catch (Exception e) {
          System.out.println("Error while parsing log, setting default index parameters");
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
        System.out.println("hops index:" + index.toString());
        URL obj;
        HttpURLConnection conn = null;
        BufferedReader br = null;
        try {
          System.out.println("elastic url:" + "http://10.0.2.15:9200/" + HopsUtil.getProjectName() + "/logs");
          obj = new URL("http://10.0.2.15:9200/" + HopsUtil.getProjectName() + "/logs");
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
          System.out.println("output:" + output);
        } finally {
          if (conn != null) {
            conn.disconnect();
          }
          obj = null;
          if (br != null) {
            br.close();
          }
        }

        return index;
      }
    });

    //jsons.print();
    //Convert json to DataFrame and then store it as parquet
    JavaDStream<LogEntry> logEntries = jsons.map(
        new Function<JSONObject, LogEntry>() {
      @Override
      public LogEntry call(JSONObject json) throws SchemaNotFoundException, MalformedURLException, ProtocolException,
          IOException {
        LogEntry logEntry = new LogEntry();
        logEntry.setMessage(json.getString("message"));
        logEntry.setPriority(json.getString("priority"));
        return logEntry;
      }
    });

   logEntries.print();

    logEntries.foreachRDD(
        new VoidFunction2<JavaRDD<LogEntry>, Time>() {
      @Override
      public void call(JavaRDD<LogEntry> rdd, Time time) throws
          Exception {
        Dataset<Row> row = session.createDataFrame(rdd, LogEntry.class);
        row.write().mode(SaveMode.Append).parquet("/Projects/yanzi2/Resources/Parquet");
      }
    });

//    JavaDStream<String> words = lines.flatMap(
//        new FlatMapFunction<String, String>() {
//      @Override
//      public Iterator<String> call(String x) {
//        return Arrays.asList(SPACE.split(x)).iterator();
//      }
//    });
//
//   
//    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
//        new PairFunction<String, String, Integer>() {
//      @Override
//      public Tuple2<String, Integer> call(String s) {
//        return new Tuple2<>(s, 1);
//      }
//    }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//          @Override
//          public Integer call(Integer i1, Integer i2) {
//            return i1 + i2;
//          }
//        });
//
//
//    /*
//     * Based on Spark Design patterns
//     * http://spark.apache.org/docs/latest/streaming-programming-guide.html#output-operations-on-dstreams
//     */
//    wordCounts.foreachRDD(
//        new VoidFunction2<JavaPairRDD<String, Integer>, Time>() {
//      @Override
//      public void call(JavaPairRDD<String, Integer> rdd, Time time) throws
//          Exception {
//        //Keep the latest microbatch output in the file
//        rdd.repartition(1).saveAsHadoopFile(args[1] + "-" + appId,
//            String.class,
//            String.class,
//            TextOutputFormat.class);
//      }
//
//    });

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
    jssc.awaitTermination();

  }

  public static boolean isShutdownRequested() {
    try {
      System.out.println("isshutdownrequested:" + HopsUtil.getProjectName());
      Configuration hdConf = new Configuration();
      Path hdPath = new org.apache.hadoop.fs.Path(
          "/Projects/" + HopsUtil.getProjectName() + "/Resources/.marker-producer-appId.txt");
      FileSystem hdfs = hdPath.getFileSystem(hdConf);
      return !hdfs.exists(hdPath);
    } catch (IOException ex) {
      Logger.getLogger(StreamingKafkaElastic.class.getName()).log(Level.SEVERE, null,
          ex);
    }
    return false;
  }
}
