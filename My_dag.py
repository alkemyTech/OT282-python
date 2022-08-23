from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta
import logging

logging.basicConfig(filename='My_dag',level=logging.DEBUG, format='%(asctime)s - %(name)s - %(message)s',datefmt='%Y-%m-%d')

def extract():
    logging.debug("Extrayendo los datos")
    return "Extracting"

def transfomr():
    return "Transforming"

def load():
    return "Loading"


default_args= {
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


with DAG(
    "etl_dag", 
    start_date=datetime(2022,1,1),
    schedule_interval='0 * * * *',
    default_args= default_args,
    catchup=False) as dag:

        #Para la universidad de Flores
        task_A = PythonOperator(
            task_id="Extracting",
            python_callable=extract,
            retries = 5,
            retry_delay= timedelta(minutes=2),
        )
        
        task_B = PythonOperator(
            task_id="Transform",
            python_callable=transfomr
        )        
        
        task_C = PythonOperator(
            task_id='Load',
            python_callable=load,
        )

        #Para la universidad de Villa Maria
        task_D = PythonOperator(
            task_id="Extracting_b",
            python_callable=extract,
            retries= 5,
            retry_delay= timedelta(minutes=2),
        )
        
        task_E = PythonOperator(
            task_id="Transform_b",
            python_callable=transfomr
        )
        
        task_F = PythonOperator(
            task_id='Load_b',
            python_callable=load,
        )


        #Una vez se van finalizando las tareas
        extraido = BashOperator(
            task_id='extracted',
            bash_command="echo 'extraido'",
        )
        
        transformado = BashOperator(
            task_id='transformado',
            bash_command="echo 'transformado'",
        )
        
        cargado = BashOperator(
            task_id='loaded',
            bash_command="echo 'cargado'",
        )


task_A >> extraido >> task_B >> transformado >> task_C >> cargado
task_D >> extraido >> task_E >> transformado >> task_F >> cargado