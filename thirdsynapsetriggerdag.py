"""Trigger Dags #1 and #2 and do something if they succeed."""
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import logging
from datetime import datetime, timedelta

with DAG(
  dag_id="thirdsynapseDAG",
  start_date=datetime(2023, 1, 1),
  schedule="@once",
) as dag:
  start_task = BashOperator(
    task_id="start_task",
    bash_command="echo 'Start task'",
  )
  trigger_A = TriggerDagRunOperator(
    task_id="trigger_first",
    trigger_dag_id="firstsynapseDAG",
    conf={"message":"Message to pass to downstream DAG A."},
  )
  trigger_B = TriggerDagRunOperator(
    task_id="trigger_B",
    trigger_dag_id="secondsynapseDAG",
  )
  start_task >> trigger_A >> trigger_B
