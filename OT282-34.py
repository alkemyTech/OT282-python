from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


logging.basicConfig(
    format= '%(asctime)s - %(name)s - %(message)s',
    level= logging.INFO,
    datefmt='%Y-%m-%d'
)

logger = logging.getLogger('Dag logear eventos Universidades')

default_args = {
        'owner': 'Alkemy',
        'depends_on_past': False,
        'email': ['lbsilvina@live.com.ar'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
        }
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
    