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
from airflow.operators.python import PythonOperator
from datetime import time
from synapsepkg.azureSynapseOperators import *
import yaml
import sys
import os
from dotenv import load_dotenv
load_dotenv()

tenantid = os.getenv('TENANTID')
clientid = os.getenv('CLIENTID')
clientsecret = os.getenv('CLIENTSECRET')
endpoint = 'px-synapse-poc'
pipelinename = ''


def task1(pipelinename):
    #run the pipeline
    #pipelinename=kwargs['pipelinename']
    res = createrun(endpoint, pipelinename, tenantid, clientid, clientsecret)
    print(f"The run ID for the pipeline run is :{res}")
    #context['ti'].xcom_push(key='runid', value=res)

def task2(pipelinename, **context):
    """ The pipeline name has to be appended to the task in the Batch DAG for the xcom push to pick up correct args"""
    #get the pipeline details
    #pipelinename=kwargs['pipelinename']
    res = context['ti'].xcom_pull(key='runid', task_ids=f'firsttask_{pipelinename}')
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
    context['ti'].xcom_push(key='status', value=status)
    return status

def check_status(pipelinename, **context):
    """ The pipeline name has to be appended to the task in the Batch DAG for the xcom push to pick up correct args"""
    #pipelinename=kwargs['pipelinename']
    status = context['ti'].xcom_pull(key='status', task_ids=f'secondtask_{pipelinename}')
    if status == 'Succeeded':
       return 0
    else:
       print(f"There was an error in task_{pipelinename}. The pipeline- {pipelinename} didnt complete successfully")
       sys.exit(1)

# Initialize the Batch by reading the yaml file and getting all pipelines to run dynamically
with open('/home/devairflow7/dags/synapseairflowpoc-main/business_critical_pipelines_batch.yml') as file:
    try:
        data = yaml.safe_load(file)
    except yaml.YAMLError as exception:
        print(exception)

default_args= {"owner":"airflow", "start_date":datetime(2023, 9, 7, 13, 0, 0), "provide_context":True}

with DAG(dag_id="business_critical_batch_dynamic_updated", default_args=default_args, schedule_interval="@once", catchup=False, is_paused_upon_creation=True
         ) as dag:
    #this bash command creates a SIG file that will be used by subsequent Batches
    start_task = EmptyOperator(task_id="start_task", dag=dag, depends_on_past=True, ignore_first_depends_on_past=True)
    #clear_sig_file = BashOperator(task_id='delete_sig_file', bash_command=f"rm -f /tmp/Batch1_completed*.SIG")
    #touch_sig_file = BashOperator(task_id='touch_sig_file', bash_command=f"touch /tmp/Batch1_completed_{current_date}_{hr}{near15}.SIG", dag=dag)
    end_task = EmptyOperator(task_id="end_task", dag=dag, depends_on_past=True, ignore_first_depends_on_past=True)
    calling_task1 = PythonOperator.partial(task_id='firsttask', python_callable=task1).expand(op_args=[[data['pipelines']]])
    #calling_task2 = PythonOperator.partial(task_id='secondtask', python_callable=task2).expand(op_args=data['pipelines'])
    #calling_check_status = PythonOperator.partial(task_id='check_status', python_callable=check_status).expand(op_args=data['pipelines'])
    start_task >> calling_task1 #>> calling_task2 >> calling_check_status >> end_task