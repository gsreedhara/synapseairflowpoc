# ------------------------------------------------------------------------------------------------------ #
# -- Author: Giridhar
# -- DAG Name: batch_template.py
# -- Date: Sep 25, 2023
# -- Desc: This Batch DAG is used to dynamically call pipelines(tasks) by passing pipeline names.
# --       This is the main template that can be used to create other batch pipelines.
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
import time
import yaml
import sys
import os
from dotenv import load_dotenv
load_dotenv()

tenantid = os.getenv('TENANTID')
clientid = os.getenv('CLIENTID')
clientsecret = os.getenv('CLIENTSECRET')
endpoint = 'px-synapse-poc'
pipelinename = '' #placeholder for the pipeline name variable


def task1(pipelinename):
    #run the pipeline
    #pipelinename=kwargs['pipelinename']
    #call the createrun from synapsepkg.azureSynapseOperators package
    res = createrun(endpoint, pipelinename, tenantid, clientid, clientsecret)
    status = ''
    #check the status of the run via the run ID
    if 'Error' in res:
        print("There was an error while running the pipeline. RunID was not generated")
        sys.exit(1)
    while status != 'Succeeded':
        res_pipelineruns = getpipelinerun(endpoint, res, tenantid, clientid, clientsecret)
        print(f"The status of the pipeline run for runID: {res} is: {res_pipelineruns}")
        status = res_pipelineruns
        time.sleep(60)
    else:
        print(status)
    if status == 'Succeeded':
       return 0
    else:
       print(f"There was an error in task_{pipelinename}. The pipeline- {pipelinename} didnt complete successfully")
       sys.exit(1)

# Initialize the Batch by reading the yaml file and getting all pipelines to run dynamically
with open('/home/devairflow7/dags/synapseairflowpoc-main/batch_pipelines.yml') as file:   #change the path of this file
    try:
        data = yaml.safe_load(file)
    except yaml.YAMLError as exception:
        print(exception)

#set default arguments for the DAG
default_args= {"owner":"airflow", "start_date":datetime(2023, 9, 7, 13, 0, 0), "provide_context":True}

#setting the DAG   #change name of dag_id corresponding to the batch_name
with DAG(dag_id="critical_jde_batch", default_args=default_args, schedule_interval="@once", catchup=False, is_paused_upon_creation=True
         ) as dag:
    #this bash command creates a SIG file that will be used by subsequent Batches
    for pipeline in data['business_critical_batch']: #change the name of the batch here corresponding to name in yml file for batch
            calling_task1 = PythonOperator(
                task_id=f'paralleltask_{pipeline}', 
                python_callable=task1, #Need to configure access to smtp server. Need to explore this
                #email=["jtoro@pxltd.ca", "gvailoces@pxltd.ca", "gsreedhara@pxltd.ca", "nning@pxltd.ca"], #change to DL list
                #email_on_failure=True,
                #email_on_retry=True,
                op_args=[pipeline])
            calling_task1
