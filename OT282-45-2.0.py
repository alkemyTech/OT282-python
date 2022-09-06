
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

def extract():
    
    user_name = config('USER_NAME')
    password = config('PASSWORD')
    host = config('HOST')
    server_ = config('PORT')
    database = config('DB_NAME')
    
    engine = create_engine(f'postgresql+psycopg2://{user_name}:{password}@{host}:{server_}/{database}')

    #For Flores University

    df_flores= pd.read_sql_query("Univeridad_de_Flores", engine)
    df_flores.to_csv('DAGS/datos_Flores')
    
    #For Villa Maria University

    df_maria= pd.read_sql_query("Univeridad_de_Flores", engine)
    df_flores.to_csv('DAGS/datos_Flores')
      
#Default DAG'a Arguments 
default_args= {
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

#Defining the DAG 
with DAG(
    dag_id="dag_base_datos_45_2.0", 
    start_date=datetime(2022,7,7),
    schedule_interval='0 * * * *',
    default_args= default_args,
    catchup=False) as dag:
          
    task_A = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    task_A