import pendulum
from pytz import UTC
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args= {"owner":"airflow", "start_date":datetime(2023, 9, 7, 13, 0, 0)}

current_date = datetime.today().strftime('%Y-%m-%d')

with DAG(dag_id="batch1_file_generation", default_args=default_args, schedule_interval="@once", catchup=False
         ) as dag:
    #this bash command creates a SIG file that will be used by subsequent Batches
    start_task = EmptyOperator(task_id="start_task", dag=dag)
    touch_sig_file = BashOperator(task_id='touch_sig_file', bash_command=f"touch /tmp/Batch1_completed_{current_date}.SIG", dag=dag)

start_task >> touch_sig_file