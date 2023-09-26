# ------------------------------------------------------------------------------------------------------ #
# -- Author: Giridhar
# -- Date: Sep 25, 2023
# -- Desc: This common mudule is used to dynamically call pipelines(tasks) by passing pipeline names. 
# --       through a main batch DAG
# ------------------------------------------------------------------------------------------------------ #

import sys

def task1(ti):
    #run the pipeline
    res = createrun(endpoint, pipelinename, tenantid, clientid, clientsecret)
    print(f"The run ID for the pipeline run is :{res}")
    ti.xcom_push(key='runid', value=res)

def task2(ti):
    """ The pipeline name has to be appended to the task in the Batch DAG for the xcom push to pick up correct args"""
    #get the pipeline details
    res = ti.xcom_pull(key='runid', task_ids=f'firsttask_{pipelinename}')
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
    """ The pipeline name has to be appended to the task in the Batch DAG for the xcom push to pick up correct args"""
    status = ti.xcom_pull(key='status', task_ids=f'secondtask_{pipelinename}')
    if status == 'Succeeded':
       return 0
    else:
       print(f"There was an error in task_{pipelinename}. The pipeline- {pipelinename} didnt complete successfully")
       sys.exit(1)