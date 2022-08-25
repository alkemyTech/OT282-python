from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def process():
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

   
    process_1 = PythonOperator(
        task_id='task_1',
        python_callable= process,
        dag=dag
    )
    process_1

    process_2 = PythonOperator(
        task_id='task_2',
        python_callable= process,
        dag=dag
    )
    process_2

    process_3 = PythonOperator(
        task_id='task_3',
        python_callable= process,
        dag=dag
    )
    process_3