from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import requests
from datetime import timedelta, datetime
import time
import sys
from synapsepkg.azureSynapseOperators import *
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import logging
from dotenv import load_dotenv
load_dotenv()

tenantid = os.getenv('TENANTID')
clientid = os.getenv('CLIENTID')
clientsecret = os.getenv('CLIENTSECRET')
endpoint = 'px-synapse-poc'
pipelinename = 'third_Batch1_pipeline_scd2'

def task1(ti):
    #run the pipeline
    res = createrun(endpoint, pipelinename, tenantid, clientid, clientsecret)
    print(f"The run ID for the pipeline run is :{res}")
    ti.xcom_push(key='runid', value=res)

def task2(ti):
    #get the pipeline details
    res = ti.xcom_pull(key='runid', task_ids='firsttask')
    status = ''
    if 'Error' in res:
        print("There was an error while running the pipeline. RunID was not generated")
        sys.exit(1)
    while status != 'Succeeded':
        res_pipelineruns = getpipelinerun(endpoint, res, tenantid, clientid, clientsecret)
        print(f"The status of the pipeline run for runID: {res} is: {res_pipelineruns}")
        status = res_pipelineruns
        time.sleep(10)
    else:
        print(status)
    ti.xcom_push(key='status', value=status)
    return status
def check_status(ti):
    status = ti.xcom_pull(key='status', task_ids='secondtask')
    if status == 'Succeeded':
       return 0
    else:
       print("There was an error in task2. The pipeline didnt complete successfully")
       sys.exit(1)                                                                                                                                                             19,1          Top
       print("There was an error in task2. The pipeline didnt complete successfully")
       sys.exit(1)


default_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'start_date' : datetime(2023, 1, 1),
    'retries': 0
}
dag = DAG(dag_id='thirdsynapseDAG', default_args=default_args, catchup=False, schedule_interval='@once')


first_task = PythonOperator(task_id='firsttask', dag=dag, python_callable=task1, do_xcom_push=True)
second_task = PythonOperator(task_id='secondtask', dag=dag, python_callable=task2, do_xcom_push=True)
check_status = PythonOperator(task_id='check_task', dag=dag, python_callable=check_status, do_xcom_push=False)
#trigger_dependency = TriggerDagRunOperator(task_id='trigger_dependency', dag=dag, trigger_dag_id='secondsynapseDAG',)


first_task >> second_task >> check_status #>> trigger_dependency