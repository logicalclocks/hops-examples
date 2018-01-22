package io.hops.examples.spark;

import io.hops.util.exceptions.CredentialsNotFoundException;
import io.hops.util.HopsUtil;
import io.hops.util.WorkflowManager;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.core.Response;
import org.apache.spark.sql.SparkSession;

/**
 * Example demo showing how to build Spark job workflows in Hopsworks. In particular, it expected that two Spark
 * jobs are already created in the project
 * <p>
 */
public class WorkflowExample {

  private static final Logger LOG = Logger.getLogger(WorkflowManager.class.getName());

  public static void main(String[] args) throws CredentialsNotFoundException, InterruptedException {

    SparkSession spark = SparkSession
        .builder()
        .appName(HopsUtil.getJobName())
        .getOrCreate();

    //Start job with ID: 6145 that prepares data for job with id 6146
    Response resp = WorkflowManager.startJobs(Integer.parseInt(args[0]));
    if (resp.getStatus() != Response.Status.OK.getStatusCode()) {
      LOG.log(Level.SEVERE, "Job could not be started");
      System.exit(1);
    }
    //Wait for job with ID: 6145 to complete
    WorkflowManager.waitForJobs(Integer.parseInt(args[0]));

    //Start job with ID: 6146
    WorkflowManager.startJobs(Integer.parseInt(args[1]));

    //Stop spark session
    spark.stop();

  }

}
