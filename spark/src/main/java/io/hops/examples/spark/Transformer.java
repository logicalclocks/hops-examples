package io.hops.examples.spark;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
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
    SparkSession spark = SparkSession.builder().getOrCreate();
    JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

    if (args[4].equalsIgnoreCase("position")) {
      JavaRDD<CSVPosition> csvEntries = lines.map(new Function<String, List<String>>() {
        @Override
        public List<String> call(String row) {
          return Arrays.asList(row.trim().split(","));
        }
      }).filter(new Function<List<String>, Boolean>() {
        @Override
        public Boolean call(List<String> split) throws Exception {
          if (split.get(0).trim().equals(args[2]) || split.get(0).trim().startsWith(args[3])) {
            return false;
          }
          return true;
        }
      }).map(new Function<List<String>, CSVPosition>() {
        @Override
        public CSVPosition call(List<String> row) {
          CSVPosition csvPosition = null;
          try {
            csvPosition = new CSVPosition(Long.parseLong(row.get(0).trim()),
                Long.parseLong(row.get(1).trim()),
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
          } catch (RuntimeException e) {
            LOG.log(Level.SEVERE, "Error while reading row from file:" + e.getMessage());
          } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error while reading row from file:" + e.getMessage());
          }
          if (csvPosition == null) {
            csvPosition = new CSVPosition(0, 0, 0.0, 0.0, 0, 0, "0", "0", "0");
          }
          return csvPosition;
        }
      });

      Dataset<Row> row = spark.createDataFrame(csvEntries, CSVPosition.class);
      row.write().mode(SaveMode.Append).parquet(args[1]);
    } else if (args[4].equalsIgnoreCase("message")) {
      JavaRDD<CSVMessage> csvEntries = lines.map(new Function<String, List<String>>() {
        @Override
        public List<String> call(String row) {
          return Arrays.asList(row.trim().split(","));
        }
      }).filter(new Function<List<String>, Boolean>() {
        @Override
        public Boolean call(List<String> split) throws Exception {
          if (split.get(0).trim().equals(args[2]) || split.get(0).trim().startsWith(args[3])) {
            return false;
          }
          return true;
        }
      }).map(new Function<List<String>, CSVMessage>() {
        @Override
        public CSVMessage call(List<String> row) {
          CSVMessage csvMessage = null;
          try {
            csvMessage = new CSVMessage(Long.parseLong(row.get(0).trim()),
                Long.parseLong(row.get(1).trim()),
                row.get(2).trim(),
                row.get(3).trim(),
                Double.parseDouble(row.get(4).trim()),
                Double.parseDouble(row.get(5).trim()),
                Integer.parseInt(row.get(6).trim()),
                Integer.parseInt(row.get(7).trim()),
                Long.parseLong(row.get(8).trim()),
                Long.parseLong(row.get(9).trim()),
                Integer.parseInt(row.get(10).trim()),
                Integer.parseInt(row.get(11).trim()),
                Integer.parseInt(row.get(12).trim()),
                Integer.parseInt(row.get(13).trim()));
          } catch (RuntimeException e) {
            LOG.log(Level.SEVERE, "Error while reading row from file:" + e.getMessage());
          } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error while reading row from file:" + e.getMessage());
          }
          if (csvMessage == null) {
            csvMessage = new CSVMessage(0, 0, "0", "0", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
          }
          return csvMessage;
        }
      });

      Dataset<Row> row = spark.createDataFrame(csvEntries, CSVMessage.class);
      row.write().mode(SaveMode.Append).parquet(args[1]);
    }
    LOG.info("Wrote parquet");
    spark.stop();
  }
}
