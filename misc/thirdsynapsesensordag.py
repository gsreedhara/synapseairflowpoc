# This DAG will use the external task sensor to sense if the first and second dags
# have completed successfully before launching third task

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    dag_id="thirdsynapsedag_sensor",
    start_date=datetime(2023, 1, 1),
    schedule='@once',
) as dag:
  start_task = BashOperator(
    task_id="start_task",
    bash_command="echo 'Start task'",
  )
  sensor_firstdag = ExternalTaskSensor(
      task_id="firsttasksensor",
      external_dag_id="firstsynapseDAG",
      external_task_id="secondtask",
      execution_delta=timedelta(minutes=10),
      timeout=300,
      poke_interval=60,
      check_existence=True
  )
  sensor_seconddag = ExternalTaskSensor(
      task_id="secondtasksensor",
      external_dag_id="secondsynapseDAG",
      external_task_id="secondtask",
      execution_delta=timedelta(minutes=10),
      timeout=300,
      poke_interval=60,
      check_existence=True
  )
  end_task = BashOperator(
      task_id="end_task",
      bash_command="echo 'End task'",
  )
  start_task >> sensor_firstdag >> sensor_seconddag >> end_task