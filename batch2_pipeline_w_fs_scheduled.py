import pendulum
from pytz import UTC
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import os
import glob 

default_args= {"owner":"airflow", "start_date":datetime(2023, 9, 7, 13, 0, 0)}

def round_dt(dt, detla):
    diff = detla * round(dt/delta)
    if diff >=60:
        diff = 0
    return diff

current_date = datetime.today().strftime('%Y%m%d')
delta = 15
mins = datetime.now().minute

near15 = datetime.now().hour + round_dt(mins,delta)
past15 = near15 - 15

# get the last 15 minutes of the current time and get the hour. 
hr = 0
if mins <= 0:
    hr = datetime.now().hour - 1
else:
    hr = datetime.now().hour



with DAG(dag_id="batch2_file_sensor", default_args=default_args, schedule_interval="*/45 * * * *", catchup=False, is_paused_upon_creation=True
         ) as dag:
    #this bash command creates a SIG file that will be used by subsequent Batches
    start_task = EmptyOperator(task_id="start_task", dag=dag, depends_on_past=True, ignore_first_depends_on_past=True)
    file_sensor = FileSensor(task_id="wait_for_file", filepath=f"/tmp/Batch1_completed_{current_date}_{hr}*.SIG", dag=dag)
    end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> file_sensor >> end_task
