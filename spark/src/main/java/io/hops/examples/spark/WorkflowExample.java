package io.hops.examples.spark;

import io.hops.util.exceptions.CredentialsNotFoundException;
import io.hops.util.HopsUtil;
import io.hops.util.WorkflowManager;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.core.Response;
import org.apache.spark.sql.SparkSession;

/**
 * Example demo showing how to build Spark job workflows in Hopsworks. In particular, it expects that two Spark
 * jobs are already created in the project. It then starts the first job, waits for it to finish and then starts the
 * second one.
 * <p>
 */
public class WorkflowExample {

  private static final Logger LOG = Logger.getLogger(WorkflowManager.class.getName());

  public static void main(String[] args) throws CredentialsNotFoundException, InterruptedException {

    SparkSession spark = SparkSession
        .builder()
        .appName(HopsUtil.getJobName())
        .getOrCreate();

    //if Start job with given ID
    Response resp = WorkflowManager.startJobs(Integer.parseInt(args[0]));
    if (resp.getStatus() != Response.Status.OK.getStatusCode()) {
      LOG.log(Level.SEVERE, "Job could not be started");
      System.exit(1);
    }
    //Wait for previous job to complete
    WorkflowManager.waitForJobs(Integer.parseInt(args[0]));

    //Start second job
    WorkflowManager.startJobs(Integer.parseInt(args[1]));

    //Stop spark session
    spark.stop();

  }

}
