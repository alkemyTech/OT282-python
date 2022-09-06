
from dataclasses import replace
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime, timedelta
import logging
from sqlalchemy import create_engine
import pandas as pd
from decouple import config



logging.basicConfig(filename='My_dag',level=logging.DEBUG, format='%(asctime)s - %(name)s - %(message)s',datefmt='%Y-%m-%d')

def load():

    s3_hook= S3Hook(aws_conn_id='conn_s3')
    s3_hook.load_file(
            filename="dags/datos_Maria.csv",
            key="datos_Maria.csv", 
            bucket_name="cohorte-agosto-38d749a7",
            replace=True
        )
  
#Default DAG'a Arguments 
default_args= {
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

#Defining the DAG 
with DAG(
    dag_id="dag_base_datos_70", 
    start_date=datetime(2022,7,7),
    schedule_interval='0 * * * *',
    default_args= default_args,
    catchup=False) as dag:
          
    task_A = PythonOperator(
        task_id="run_s3",
        python_callable=load
    )

    task_A