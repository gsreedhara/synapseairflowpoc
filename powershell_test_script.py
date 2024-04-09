# ------------------------------------------------------------------------------------------------------ #
# -- Author: Giridhar
# -- DAG Name: powershell_test_dag.py
# -- Date: Sep 25, 2023
# -- Desc: This DAG is used to schedule powershell scripts on a remote system
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
import itertools
from airflow.models.baseoperator import chain_linear
from airflow.models.baseoperator import chain
from airflow.providers.microsoft.psrp.operators.psrp import PsrpOperator

default_args = {"psrp_conn_id":"psrp_test_conn"}

with DAG (
    dag_id="psrp_test", 
    schedule="@once"
    start_date=datetime(2024,2,2),
    catchup=False,
) as dag:
    new_psrp = PsrpOperator(
        task_id="TestConn"m
        psrp_conn_id="psrp_test_conn",
        command="powershell .\test_Giri.ps1"
    )

new_psrp


