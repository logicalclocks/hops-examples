import airflow

from datetime import datetime, timedelta
from airflow import DAG

from hopsworks_plugin.operators.hopsworks_operator import HopsworksLaunchOperator
from hopsworks_plugin.operators.hopsworks_operator import HopsworksFeatureValidationResult
from hopsworks_plugin.sensors.hopsworks_sensor import HopsworksJobSuccessSensor

# Username in Hopsworks
# Click on Account from the top right drop-down menu
DAG_OWNER = 'meb10000'

# Project name this DAG belongs to
PROJECT_NAME = 'ExtremeEarth'


####################
## DAG definition ##
####################
delta = timedelta(minutes=-10)
now = datetime.now()

args = {
    'owner': DAG_OWNER,
    'depends_on_past': False,

    # DAG should have run 10 minutes before now
    # It will be automatically scheduled to run
    # when we upload the file in Hopsworks
    'start_date': now + delta,

    # Uncomment the following line if you want Airflow
    # to authenticate to Hopsworks using API key
    # instead of JWT
    #
    # NOTE: Edit only YOUR_API_KEY
    #
    
}

# Our DAG
dag = DAG(
    # Arbitrary identifier/name
    dag_id = "iceberg_pipeline",
    default_args = args,

    # Run the DAG only one time
    # It can take Cron like expressions
    # E.x. run every 30 minutes: */30 * * * * 
    schedule_interval = "@once"
)



launch_Stage1_Preprocessing = HopsworksLaunchOperator(dag=dag,
					 project_name="ExtremeEarth",
					 task_id="launch_Stage1_Preprocessing",
					 job_name="Stage1_Preprocessing",
					 wait_for_completion=True)
					 

wait_Stage1_Preprocessing = HopsworksJobSuccessSensor(dag=dag,
					   project_name="ExtremeEarth",
                                   	   task_id="wait_Stage1_Preprocessing",
                                   	   job_name="Stage1_Preprocessing")


launch_Stage2_FeatureStore = HopsworksLaunchOperator(dag=dag,
					 project_name="ExtremeEarth",
					 task_id="launch_Stage2_FeatureStore",
					 job_name="Stage2_FeatureStore",
					 wait_for_completion=True)
					 

wait_Stage2_FeatureStore = HopsworksJobSuccessSensor(dag=dag,
					   project_name="ExtremeEarth",
                                   	   task_id="wait_Stage2_FeatureStore",
                                   	   job_name="Stage2_FeatureStore")


launch_Stage3_Training = HopsworksLaunchOperator(dag=dag,
					 project_name="ExtremeEarth",
					 task_id="launch_Stage3_Training",
					 job_name="Stage3_Training",
					 wait_for_completion=True)
					 

wait_Stage3_Training = HopsworksJobSuccessSensor(dag=dag,
					   project_name="ExtremeEarth",
                                   	   task_id="wait_Stage3_Training",
                                   	   job_name="Stage3_Training")


launch_Stage4_ModelServing = HopsworksLaunchOperator(dag=dag,
					 project_name="ExtremeEarth",
					 task_id="launch_Stage4_ModelServing",
					 job_name="Stage4_ModelServing",
					 wait_for_completion=True)
					 

wait_Stage4_ModelServing = HopsworksJobSuccessSensor(dag=dag,
					   project_name="ExtremeEarth",
                                   	   task_id="wait_Stage4_ModelServing",
                                   	   job_name="Stage4_ModelServing")


launch_Stage5_ModelMonitoring = HopsworksLaunchOperator(dag=dag,
					 project_name="ExtremeEarth",
					 task_id="launch_Stage5_ModelMonitoring",
					 job_name="Stage5_ModelMonitoring",
					 wait_for_completion=True)
					 

wait_Stage5_ModelMonitoring = HopsworksJobSuccessSensor(dag=dag,
					   project_name="ExtremeEarth",
                                   	   task_id="wait_Stage5_ModelMonitoring",
                                   	   job_name="Stage5_ModelMonitoring")



wait_Stage1_Preprocessing.set_upstream(launch_Stage1_Preprocessing)
launch_Stage2_FeatureStore.set_upstream(wait_Stage1_Preprocessing)
wait_Stage2_FeatureStore.set_upstream(launch_Stage2_FeatureStore)
launch_Stage3_Training.set_upstream(wait_Stage2_FeatureStore)
wait_Stage3_Training.set_upstream(launch_Stage3_Training)
launch_Stage4_ModelServing.set_upstream(wait_Stage3_Training)
wait_Stage4_ModelServing.set_upstream(launch_Stage4_ModelServing)
launch_Stage5_ModelMonitoring.set_upstream(wait_Stage4_ModelServing)
wait_Stage5_ModelMonitoring.set_upstream(launch_Stage5_ModelMonitoring)
