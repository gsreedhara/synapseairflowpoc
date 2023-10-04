# ------------------------------------------------------------------------------------------------------ #
# -- Author: Giridhar
# -- Date: Sep 25, 2023
# -- Desc: This Batch DAG is used to dynamically call pipelines(tasks) by passing pipeline names. 
# --       This is the main batch for Business Critical Pipelines. 
# --       This DAG uses a YAML file to cycle through the pipelines and call pipelines dynamically
# --       at runtime
# ------------------------------------------------------------------------------------------------------ #

import pendulum
from pytz import UTC
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import yaml

default_args= {"owner":"airflow", "start_date":datetime(2023, 9, 7, 13, 0, 0)}

current_date = datetime.today().strftime('%Y%m%d')

def round_dt(dt, detla):
    diff = detla * round(dt/delta)
    if diff >=60:
        diff = 0
    return diff


delta = 15

dt = datetime.now().minute
print(f"the SIG file will be created for time {round_dt(dt,delta)}")
near15 = datetime.now().hour + round_dt(dt,delta)
hr = datetime.now().hour
with DAG(dag_id="batch1_file_generation", default_args=default_args, schedule_interval="*/15 * * * *", catchup=False, is_paused_upon_creation=True
         ) as dag:
    #this bash command creates a SIG file that will be used by subsequent Batches
    start_task = EmptyOperator(task_id="start_task", dag=dag, depends_on_past=True, ignore_first_depends_on_past=True)
    clear_sig_file = BashOperator(task_id='delete_sig_file', bash_command=f"rm -f /tmp/Batch1_completed*.SIG")
    touch_sig_file = BashOperator(task_id='touch_sig_file', bash_command=f"touch /tmp/Batch1_completed_{current_date}_{hr}{near15}.SIG", dag=dag)

start_task >> clear_sig_file >> touch_sig_file


with open('/Users/giridharsreedhara/Library/CloudStorage/OneDrive-ProjectXLtd/My Documents/pythonprojects/git repos/synapseairflowpoc/business_critical_pipelines_batch.yml') as file:
    try:
        data = yaml.safe_load(file)
        print(type(data))
        print(data)
        for pipeline in data['pipelines']:
            print(value)
    except yaml.YAMLError as exception:
        print(exception)