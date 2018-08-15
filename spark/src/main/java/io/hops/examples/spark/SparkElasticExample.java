/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.hops.examples.spark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.json.JSONObject;

/**
 * SparkPi for Hops.
 * <p>
 */
public class SparkElasticExample {
  private static final Logger LOG = Logger.getLogger(SparkElasticExample.class.getName());

  public static void main(String[] args) throws Exception {
    
    SparkConf sparkConf = new SparkConf().setAppName("SparkElasticExample");

    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
    int n = 10 * slices;
    List<Integer> l = new ArrayList<Integer>(n);
    for (int i = 0; i < n; i++) {
      l.add(i);
    }

    JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

    int count = dataSet.map(new Function<Integer, Integer>() {
      @Override
      public Integer call(Integer integer) throws MalformedURLException, IOException {
        double x = Math.random() * 2 - 1;
        double y = Math.random() * 2 - 1;

        //Send elastic index
        /*
         * "_index" : "demo_admin000",
         * "_type" : "logs",
         * "_id" : "AVpmqnPB0FcD8Lv7opmq",
         * "_score" : 1.0,
         * "_source" : {
         * "message" : "Changing view acls to: glassfish,demo_admin000__meb10000",
         * "@version" : "1",
         * "@timestamp" : "2017-02-22T16:32:11.153Z",
         * "timestamp" : 1487781131151,
         * "path" : "org.apache.spark.SecurityManager",
         * "priority" : "INFO",
         * "logger_name" : "org.apache.spark.SecurityManager",
         * "thread" : "main",
         * "class" : "?",
         * "file" : "?:?",
         * "method" : "?",
         * "application" : "application_1487779260123_0001",
         * "host" : "10.0.2.15:32788",
         * "project" : "demo_admin000",
         * "jobname" : "Job-1487781115.434"
         * }
         */
        JSONObject index = new JSONObject();
        index.put("@version", "1");
        index.put("@timestamp", "123");
        index.put("message", "fy fan");
        index.put("timestamp", "123");
        index.put("path", "test_path");
        index.put("priority", "fy fan");
        index.put("logger_name", "fy fan");
        index.put("thread", "fy fan");
        index.put("class", "fy fan");
        index.put("file", "fy fan");
        index.put("application", "fy fan");
        index.put("host", "fy fan");
        index.put("project", "fy fan");
        index.put("method", "fy fan");
        index.put("jobname", "fy fan");

        URL obj;
        HttpURLConnection conn = null;
        BufferedReader br = null;
        try {
          obj = new URL("10.0.2.15:9200/test/logs");

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
          LOG.info("output:" + output);
        } finally {
          if (conn != null) {
            conn.disconnect();
          }
          obj = null;
          if (br != null) {
            br.close();
          }
        }

        return (x * x + y * y < 1) ? 1 : 0;
      }
    }).reduce(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer integer, Integer integer2) {
        return integer + integer2;
      }
    });

    jsc.stop();
  }

}
