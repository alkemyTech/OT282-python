from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from sqlalchemy import create_engine
import pandas as pd
import logging

logging.basicConfig(filename='app.log',
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d',
                    level=logging.DEBUG)

#Task 1
'''Lo ideal es modularizar las funciones para tener un código más legible permitiendo un fácil mantenimiento de este.
No se realizó debido a que airflow, ejecutado localmente, no me permitió importar. '''
def extract():
    try:
        querys = {'jujuy': 'OT282-15-tabla jujuy.sql',
                  'palermo': 'OT282-15-tabla palermo.sql'
                  }
        engine = create_engine('postgresql+psycopg2://alkymer2:Alkemy23@training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com:5432/training')

        for keys,values in querys.items():
            with open (values, encoding='utf-8') as query:
                query_ = query.read()
            universidad = pd.read_sql_query(query_, engine)
            name = f'universidad_{keys}.csv'
            universidad.to_csv(name)
    except:
        logging.error('Error de conexión')

#Task 2
def normalize():
    pass
        
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
    t1 = PythonOperator(
        task_id = 'query_universidad_c',
        python_callable=extract
        )
    t2 = PythonOperator(
        task_id = 'normalize_data',
        python_callable=normalize
    )
    
    t1.doc_md = dedent(
        """\ ### Función
        la tarea realiza una consulta a la tablas de Universidad_C obteniendo las columnas necesarias y procesamiento de datos"""
    )
    
    t2.doc_md = dedent(
        """\ ### Función
        la tarea realiza la carga de datos en S3"""
    )
# Configuración de dependencia
t1 >> t2
