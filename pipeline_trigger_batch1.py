import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import logging
import time


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner":"airflow",
        "start_date": airflow.utils.dates.days_ago(1)}

dag= DAG(dag_id="pipeline_trigger_batch1", default_args=args, schedule_interval='00 12 * * *')

def firsttask():
    print("Starting batch 1 pipeline")

def waittask():
    """this is to wait between task runs to give it time for the triggers to work"""
    time.sleep(30)


with dag:

    trigger_pipeline_1 = TriggerDagRunOperator(
    task_id="trigger_pipeline1",
    trigger_dag_id="firstsynapseDAG",
    conf={"message":"Message to pass to downstream DAG A."},
    execution_date='{{ ds }}',
    wait_for_completion=True,
    poke_interval=60,
    reset_dag_run=True,
    dag=dag
  )
    trigger_pipeline_2 = TriggerDagRunOperator(
    task_id="trigger_pipeline2",
    trigger_dag_id="secondsynapseDAG",
    conf={"message":"Message to pass to downstream DAG B."},
    execution_date='{{ ds }}',
    wait_for_completion=True,
    poke_interval=60,
    reset_dag_run=True,
    dag=dag
  )
    trigger_pipeline_3 = TriggerDagRunOperator(
    task_id="trigger_pipeline3",
    trigger_dag_id="thirdsynapseDAG",
    conf={"message":"Message to pass to downstream DAG C."},
    execution_date='{{ ds }}',
    wait_for_completion=True,
    poke_interval=60,
    reset_dag_run=True,
    dag=dag
  )
    first_task = PythonOperator(task_id="begin_task", python_callable=firsttask, dag=dag)
    wait_task1=PythonOperator(task_id="wait_task1", python_callable=waittask, dag=dag)
    first_task >> trigger_pipeline_1 >> wait_task >>  trigger_pipeline_2
    wait_task2=PythonOperator(task_id="wait_task2", python_callable=waittask, dag=dag)
    trigger_pipeline_3 >> wait_task2