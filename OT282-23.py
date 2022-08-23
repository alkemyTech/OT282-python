from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# Definición de DAG
with DAG(
    dag_id = 'universidades_C',
    default_args={
    'depends_on_past': False,
    'email': ['amonza621@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    },
    description= 'Hacer un ETL para 2 universidades distintas',
    schedule_interval='@daily',
    start_date=datetime(2022, 8, 21),
    catchup=False,
) as dag:
    t1 = DummyOperator(
        task_id = 'query_SQL_jujuy_utn'
        )
    t2 = DummyOperator(
        task_id = 'query_SQL_palermo' 
    )
    t3 = DummyOperator(
        task_id = 'carga_de_datos'
    )
    
    t1.doc_md = dedent(
        """\ ### Función
        la tarea realiza una consulta a la tabla jujuy_utn obteniendo las columnas necesarias y procesamiento de datos"""
    )
    t2.doc_md = dedent(
        """\ ### Función
        la tarea realiza una consulta a la tabla palermo_tres_de_febrero obteniendo las columnas necesarias y procesamiento de datos"""
    )
    t3.doc_md = dedent(
        """\ ### Función
        la tarea realiza la carga de datos en S3"""
    )
# Configuración de dependencia
    [t1, t2] >> t3