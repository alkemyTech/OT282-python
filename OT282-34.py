from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
        'owner': 'Alkemy',
        'depends_on_past': False,
        'email': ['lbsilvina@live.com.ar'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
        }