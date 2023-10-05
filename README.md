# Airflow-Synapse Orchestration

The HLD for this implementation can be found here - https://projectxltd-my.sharepoint.com/:w:/g/personal/gsreedhara_pxltd_ca/EaycIJCmqj9FuXKgMbidcusBPyKK7JpUa1xx2QelWK4pcw?e=QBC3ww

Here are the steps to setting up the environment
1. Provision the VM based on the prereq or approved configurations

a. Create an App registration - SynapseAirflowAppReg(or a name that is approved)
b. Create Client secrets and ID for this appreg
c. Create a synapse gateway that gives access on this appreg to synapse gateway- Under app registration API permissions
d. Under Synapse workspace - Manage- Access Control- Give Synapse Contributor and Synapse Compute operator permissions to this appreg

3. Install Mysql instance on the Airflow VM/server

4. Create /dag/folder if necessary for our folders. Copy the .env, .yml, batch_template.py, requirements.txt and any other file necessary. Also copy the synapsepkg wheel file from github

5. pip install -r requirements.txt to install relevent libraries/packages for airflow dags to run

6. pip install /path/to/synapsepkg.whl

7. Configure the airflow.cfg In the home/airflow folder. 

a. Change default dag folder to point to home/dag/folder path or the path where the dags are residing
b. Change the executor to localexecutor and define the path for mysql with connection details for repository metadata
c. Optionally remove the example dags so they dont show up on the WebUI or set them to false on airflow.cfg

9. Add the pipelines you want to run in the batch_pipelines.yml file under relevant header/key. This needs to be same in the Batch DAG you want to run. 

10. Run the airflow db init & airflow webserver & airflow scheduler &  to run these services in the background.
   Check airflow db init to see if any errors are returned. This will also indicate if the config and dags are correct with all necessary packages


**Note**: For demo purposes create a couple of pipelines with Wait activity of 20-30 seconds in the Dev environment. This will be faster. It can be in any folder. Since Pipeline names under a workspace is unique. Add these
to the batch_pipelines.yml file. If there is a pipeline that doesnt match the pipeline name in Synapse it will return an Error and the task will fail. 

**Note**: The pipeline names have to be without spaces. underscores and hypens are fine.   
