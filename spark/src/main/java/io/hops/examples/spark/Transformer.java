package io.hops.examples.spark;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 *
 * <p>
 */
public class Transformer {

  private static final Logger LOG = Logger.getLogger(Transformer.class.getName());

  public static void main(final String[] args) throws Exception {
    SparkSession spark = SparkSession.builder().appName("test").getOrCreate();
    JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
    SimpleDateFormat formatter = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    JavaRDD<CSVEntry> csvEntries = lines.map(new Function<String, List<String>>() {
      @Override
      public List<String> call(String row) {
        return Arrays.asList(row.trim().split(","));
      }
    }).filter(new Function<List<String>, Boolean>() {
      @Override
      public Boolean call(List<String> split) throws Exception {
        if (split.get(0).trim().equals(args[1]) || split.get(0).trim().startsWith(args[2])) {
          return false;
        }
        return true;
      }
    }).map(new Function<List<String>, CSVEntry>() {
      @Override
      public CSVEntry call(List<String> row) throws Exception {
        DateFormat df = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss.SSS");
        return new CSVEntry(Integer.parseInt(row.get(0).trim()),
            Integer.parseInt(row.get(1).trim()),
            Double.parseDouble(row.get(2).trim()),
            Double.parseDouble(row.get(3).trim()),
            Integer.parseInt(row.get(4).trim()),
            Integer.parseInt(row.get(5).trim()),
            row.get(6).trim(),
            row.get(7).trim(),
            row.get(8).trim());
            //new java.sql.Date(formatter.parse(row.get(6).trim()).getTime()),
            //new java.sql.Date(formatter.parse(row.get(7).trim()).getTime()),
            //new java.sql.Date(formatter.parse(row.get(8).trim()).getTime()))
      }
    });

    Dataset<Row> row = spark.createDataFrame(csvEntries, CSVEntry.class);
    row.write().mode(SaveMode.Append).parquet("/Projects/zep1/test/Parquet");

    // DataFrames can be saved as Parquet files, maintaining the schema information
    //peopleDF.write().parquet(args[1]);
    LOG.info("Wrote parquet");
    spark.stop();
  }
}
