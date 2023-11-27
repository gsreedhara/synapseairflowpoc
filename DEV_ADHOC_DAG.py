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
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

#import logging

#Get the environment variable that will dictate what Env on Synapse these DAGS run the pipeline on"
import __main__
"""The name of the files need to be DEV_, UAT_ or PRD_"""
#read_file_name = __main__.__file__
#read_file_name = sys.argv[0]
read_file_name = Path(__file__).stem
#env = "DEV"

if "DEV" in read_file_name:
    env = "DEV"
elif "UAT" in read_file_name:
    env = "UAT"
elif "PRD" in read_file_name:
    env = "PRD"
else:
    print("The File name should begin with DEV, UAT or PRD. Defaulting to DEV to minimize impact")
    sys.exit(1)
    #env = "DEV"

import logging

logger = logging.getLogger(__name__)
logger.info("Beginning of DAG execution")

tenantid = os.getenv(f"TENANTID_{env}")
clientid = os.getenv(f'CLIENTID_{env}')
clientsecret = os.getenv(f'CLIENTSECRET_{env}')
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
        if status == 'Failed':
            get_activity_runs = queryActivityRuns(endpoint, pipelinename, res, tenantid, clientid, clientsecret)
            logger.info(f"pipeline activity status by activity runs :\n {get_activity_runs}")
            sys.exit(1)
        time.sleep(20)
    else:
        print(status)
    if status == 'Succeeded':
        #get the activity runs for the pipeline
        get_activity_runs = queryActivityRuns(endpoint, pipelinename, res, tenantid, clientid, clientsecret)
        print(f"The pipeline returned with the following message per activity {get_activity_runs}")
        logger.info(f"pipeline activity status by activity runs :\n {get_activity_runs}")
        return 0
    else:
       print(f"There was an error in task_{pipelinename}. The pipeline- {pipelinename} didnt complete successfully")
       sys.exit(1)

# Initialize the Batch by reading the yaml file and getting all pipelines to run dynamically
with open(f'/home/devairflow7/dags/synapseairflowpoc-main/{env}_batch_pipelines.yml') as file:   #change the path of this file
    try:
        data = yaml.safe_load(file)
    except yaml.YAMLError as exception:
        print(exception)

#set default arguments for the DAG
default_args= {"owner":"airflow", "start_date":datetime(2023, 9, 7, 13, 0, 0), "provide_context":True}

#setting the DAG   #change name of dag_id corresponding to the batch_namewith DAG(dag_id="critical_jde_batch", default_args=default_args, schedule_interval="@once", catchup=False, is_paused_upon_creation=True
with DAG(dag_id=f"{env}_adhoc_batch", default_args=default_args, schedule_interval="*/15 8-20 * * 1-5", catchup=False, is_paused_upon_creation=True) as dag:
    #this bash command creates a SIG file that will be used by subsequent Batches
    adhoc_list=[]
    total = len(data['Adhoc'])
    for i in range(0,total):
        pipeline = data['Adhoc'][i]
        adhoc_list.append(PythonOperator(task_id=f'dependTask_{pipeline}',python_callable=task1, op_args=[pipeline], depends_on_past=True))
        if i not in [0]:
            adhoc_list[i-1] >> adhoc_list[i]
