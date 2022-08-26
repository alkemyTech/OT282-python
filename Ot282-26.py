from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def tarea():
   
   return print ('hola')


with DAG(
    'Universidades',
    default_args=default_args,
    description='OT282-26',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 8, 23),
    catchup=False,
    tags=['Alkemy'],
) as dag:

   
    etl = PythonOperator(
        task_id='First_task_1',
        python_callable=tarea,
        dag=dag
    )
    etl
    