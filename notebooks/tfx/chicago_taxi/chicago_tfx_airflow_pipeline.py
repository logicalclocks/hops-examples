import airflow

from datetime import datetime, timedelta
from airflow import DAG

from hopsworks_plugin.operators.hopsworks_operator import HopsworksLaunchOperator
from hopsworks_plugin.sensors.hopsworks_sensor import HopsworksJobSuccessSensor

delta = timedelta(minutes=-10)
now = datetime.now()

args = {
    # Username in Hopsworks
    'owner': 'meb10000', # replace with your username if needed. Find it in by clicking in your Account in Hopsworks.
    'depends_on_past': False,

    # Start date is 10 minutes ago
    'start_date': now + delta
}

dag = DAG(
    dag_id='chicago_tfx_airflow_pipeline',
    default_args=args,

    # Run every seven minutes
    schedule_interval='*/77 * * * *'
)

# Project ID extracted from URL
PROJECT_NAME = "flink_tutorial" #replace with your project's name

compute_statistics = HopsworksLaunchOperator(dag=dag, task_id='compute_statistics', job_name='ComputeStatistics', project_name=PROJECT_NAME)
infer_schema = HopsworksLaunchOperator(dag=dag, task_id='infer_schema', job_name='InferSchema', project_name=PROJECT_NAME)
compare_statistics = HopsworksLaunchOperator(dag=dag, task_id='compare_statistics', job_name='CompareStatistics', project_name=PROJECT_NAME)
freeze_schema = HopsworksLaunchOperator(dag=dag, task_id='freeze_schema', job_name='FreezeSchema', project_name=PROJECT_NAME)
preprocess_inputs = HopsworksLaunchOperator(dag=dag, task_id='preprocess_inputs', job_name='PreprocessInputs', project_name=PROJECT_NAME)
compute_stats_transformed_data = HopsworksLaunchOperator(dag=dag, task_id='compute_stats_transformed_data', job_name='ComputeStatsTransformedData', project_name=PROJECT_NAME)
training = HopsworksLaunchOperator(dag=dag, task_id='training', job_name='Training', project_name=PROJECT_NAME)
tfma = HopsworksLaunchOperator(dag=dag, task_id='tfma', job_name='TFMA', project_name=PROJECT_NAME)

compute_statistics >> infer_schema >> compare_statistics >> freeze_schema >> preprocess_inputs >> compute_stats_transformed_data >> training >> tfma