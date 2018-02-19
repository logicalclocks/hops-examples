package io.hops.examples.spark;

import io.hops.util.Hops;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 *
 * 
 */
public class MyHiveExamples {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName(Hops.getJobName());
    SparkSession spark = SparkSession
        .builder()
        .appName(Hops.getJobName())
        .config(sparkConf)
        .getOrCreate();

    spark.read().parquet(args[0] + args[1] + ".parquet").write().partitionBy("cgi").option(
        "orc.compress", "zlib").orc(args[0] + args[2] + "_cgi.orc");
    
    spark.stop();

  }
}
