from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import requests
from datetime import timedelta, datetime
import time
from synapsepkg.azureSynapseOperators import *
from dotenv import load_dotenv
load_dotenv()

tenantid = os.getenv('TENANTID')
clientid = os.getenv('CLIENTID')
clientsecret = os.getenv('CLIENTSECRET')

def task1(ti):
    #run the pipeline
    res = createrun('test-workspace-airflow-poc', 'samplepipeline2', tenantid, clientid, clientsecret)
    print(f"The run ID for the pipeline run is :{res}")
    ti.xcom_push(key='runid', value=res)

def task2(ti):
    #get the pipeline details
    res = ti.xcom_pull(key='runid', task_ids='firsttask')
    status = ''
    while status != 'Succeeded':
        res_pipelineruns = getpipelinerun('test-workspace-airflow-poc', res, tenantid, clientid, clientsecret)
        print(f"The status of the pipeline run for runID: {res} is: {res_pipelineruns}")
        status = res_pipelineruns
        time.sleep(10)
    else:
        print(status)
    return status

default_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'start_date' : datetime(2023, 1, 1),
    'retries': 0
}
dag = DAG(dag_id='secondsynapseDAG', default_args=default_args, catchup=False, schedule_interval='@once')

# step - 4

first_task = PythonOperator(task_id='firsttask', dag=dag, python_callable=task1, do_xcom_push=True)
second_task = PythonOperator(task_id='secondtask', dag=dag, python_callable=task2, do_xcom_push=True)
# step - 5

first_task >> second_task
