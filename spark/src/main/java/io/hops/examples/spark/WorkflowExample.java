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

import io.hops.util.Hops;
import io.hops.util.WorkflowManager;
import io.hops.util.exceptions.CredentialsNotFoundException;
import io.hops.util.exceptions.WorkflowManagerException;
import org.apache.spark.sql.SparkSession;


/**
 * Example demo showing how to build Spark job workflows in Hopsworks. In particular, it expects that two Spark
 * jobs are already created in the project. It then starts the first job, waits for it to finish and then starts the
 * second one.
 * <p>
 */
public class WorkflowExample {


  public static void main(String[] args)
    throws WorkflowManagerException, CredentialsNotFoundException, InterruptedException {

    SparkSession spark = SparkSession
        .builder()
        .appName(Hops.getJobName())
        .getOrCreate();

    //if Start job with given ID
    WorkflowManager.startJobs(Integer.parseInt(args[0]));

    //Wait for previous job to complete
    WorkflowManager.waitForJobs(Integer.parseInt(args[0]));

    //Start second job
    WorkflowManager.startJobs(Integer.parseInt(args[1]));

    //Stop spark session
    spark.stop();

  }

}
