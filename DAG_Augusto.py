from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from sqlalchemy import create_engine
import pandas as pd
import logging
from decouple import config
from airflow.hooks.S3_hook import S3Hook

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
        user_name = config('USER_NAME')
        password = config('PASSWORD')
        host = config('HOST')
        server_ = config('PORT')
        database = config('DB_NAME')
        engine = create_engine(f'postgresql+psycopg2://{user_name}:{password}@{host}:{server_}/{database}')

        for keys,values in querys.items():
            with open (values, encoding='utf-8') as query:
                query_ = query.read()
            universidad = pd.read_sql_query(query_, engine)
            name = f'universidad_{keys}.csv'
            universidad.to_csv(name)
        logging.info('Lectura y descarga de tablas realizado')
    except:
        logging.error('Error de conexión')

#Task 2
def transform():
    try:
        #read CSV files
        tabla_jujuy = pd.read_csv('universidad_jujuy.csv', index_col=0)
        tabla_palermo = pd.read_csv('universidad_palermo.csv', index_col=0)
        codigos_postales = pd.read_csv('codigos_postales.csv')

        #rename columns
        tabla_jujuy.rename(columns={'sexo':'gender',
                                        'nombre': 'first_name'}, inplace=True)
        tabla_palermo.rename(columns={'universidad':'university',
                                        'careers': 'career',
                                        'fecha_de_inscripcion': 'inscription_date',
                                        'names': 'first_name',
                                        'sexo': 'gender',
                                        'codigo_postal': 'postal_code',
                                        'correos_electronicos': 'email'}, inplace=True)
        codigos_postales.rename(columns={'codigo_postal': 'postal_code',
                                            'localidad': 'location'}, inplace=True)

        #Obtendo las columnas first_name y last_name
        tabla_jujuy['last_name'] = tabla_jujuy['first_name'].str.split(' ').str.get(1)
        tabla_jujuy['first_name'] = tabla_jujuy['first_name'].str.split(' ').str.get(0)
        tabla_palermo['first_name'] = tabla_palermo['first_name'].str.replace('dr.', '').str.replace('mrs', '').str.replace('mr', '').str.replace('_', ' ').str.strip()
        tabla_palermo['last_name'] = tabla_palermo['first_name'].str.split(' ').str.get(1)
        tabla_palermo['first_name'] = tabla_palermo['first_name'].str.split(' ').str.get(0)

        #Normalizo columna 'age'
        tabla_jujuy['age'] = tabla_jujuy['age'].str.split(' ').str.get(0)
        tabla_palermo['age'] = tabla_palermo['age'].str.split(' ').str.get(0)
        tabla_jujuy = tabla_jujuy.astype({'age': 'float32'})
        tabla_palermo = tabla_palermo.astype({'age': 'float32'})
        tabla_jujuy['age'] = tabla_jujuy['age']/365
        tabla_palermo['age'] = tabla_palermo['age']/365
        tabla_jujuy = tabla_jujuy.astype({'age': 'int'})
        tabla_palermo = tabla_palermo.astype({'age': 'int'})

        #Unión con datos faltantes
        codigos_postales['location'] = codigos_postales['location'].str.lower()
        jujuy = pd.merge(tabla_jujuy, codigos_postales, how='left', on='location')
        palermo = pd.merge(tabla_palermo, codigos_postales, how='left', on='postal_code')

        #Normalizado de tablas
        columns = ['university', 'career', 'first_name', 'last_name', 'location', 'email']

        for column in columns:
            jujuy[f'{column}'] = jujuy[f'{column}'].str.strip().str.lower().str.replace('_', ' ')
            palermo[f'{column}'] = palermo[f'{column}'].str.strip().str.lower().str.replace('_', ' ')

        #Elimino filas duplicadas
        jujuy = jujuy.drop_duplicates(['email', 'career'], keep='first')
        palermo = palermo.drop_duplicates(['email', 'career'], keep='first')

        #Tablas finales to_txt
        jujuy.to_csv('universidad_de_jujuy.txt')
        palermo.to_csv('universidad_de_palermo.txt')
        logging.info('Normalizado de datos con éxito')
    except:
        logging.error('No se realizó el normalizado de las tablas')

#Task 3
def load(filename: str, key: str, bucket_name: str) -> None:
    try:
        hook = S3Hook('s3_conn')
        hook.load_file(filename=filename, key=key, bucket_name=bucket_name)
        logging.info('Datos cargado con éxito')
    except:
        logging.error('Error de carga de datos a S3')
    
        
# Definición de DAG
with DAG(
    dag_id = 'a_universidades_C',
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
        python_callable=transform
    )
    t3 = PythonOperator(
        task_id = 'load_data_jujuy',
        python_callable=load,
        op_kwargs={
            
            'filename': 'universidad_de_jujuy.txt',
            'key': 'universidad_de_jujuy.txt',
            'bucket_name': 'cohorte-agosto-38d749a7'
        }
    )
    t4 = PythonOperator(
        task_id = 'load_data_palermo',
        python_callable=load,
        op_kwargs={
            
            'filename': 'universidad_de_palermo.txt',
            'key': 'universidad_de_palermo.txt',
            'bucket_name': 'cohorte-agosto-38d749a7'
        }
    )
    
    t1.doc_md = dedent(
        """\ ### Función
        la tarea realiza una consulta a la tablas de Universidad_C obteniendo las columnas necesarias y procesamiento de datos"""
    )
    
    t2.doc_md = dedent(
        """\ ### Función
        la tarea realiza el procesamiento de datos con pandas"""
    )
    t3.doc_md = dedent(
        """\ ### Función
        la tarea realiza la carga de datos en S3"""
    )
    
# Configuración de dependencia
t1 >> t2 >> t3 >> t4