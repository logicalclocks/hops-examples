import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


#
# TODO: you must replace 'dowling_' below with your username in Hopsworks.
# You can get your username by clicking on the "Account" menu item in the top-right after you have logged in.
#
args = {
    'owner': 'dowling_',
    'email': 'airflow@logicalclocks.com',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id = 'hopsworks_test_dag_jim',
    default_args = args,
    schedule_interval = '@daily'
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag=dag)

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t1)
