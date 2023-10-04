import pprint as pp
import airflow.utils.dates
from airflow import DAG 
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args ={
    "owner":"airflow",
    "start_date":"datetime(2023, 9, 6, 17, 0, 0)"
}

with DAG(dag_id="external_task_sensor_timedelta_2", default_args=default_args, schedule_interval="@daily", catchup=False
         ) as dag:
    only_task = EmptyOperator(task_id='only_task')

    only_task