package io.hops.examples.spark;

import io.hops.util.CredentialsNotFoundException;
import io.hops.util.HopsUtil;
import org.apache.spark.sql.SparkSession;

/**
 * Example demo showing how to build Spark job workflows in Hopsworks.
 * <p>
 */
public class WorkflowManager {

  public static void main(String[] args) throws CredentialsNotFoundException {

    SparkSession spark = SparkSession
        .builder()
        .appName(HopsUtil.getJobName())
        .getOrCreate();

    //Start job with ID: 6145 that prepares data for job with id 6146
    HopsUtil.startJobs(6145);

    //Wait for job with ID: 6145 to complete
    HopsUtil.waitJobs(6145);

    //Start job with ID: 6146
    HopsUtil.startJobs(6146);

    //Stop spark session
    spark.stop();

  }

}
