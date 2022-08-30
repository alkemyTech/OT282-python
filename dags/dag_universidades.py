'''
Este dag nos va a permitir extraer datos de universidades, poderlas
transformar con pandas y luego cargar los datos listos en un bucket de s3
'''
from datetime import datetime, timedelta
import logging
from airflow import DAG
#Este operator voy a utilizar para realizar las tareas de ETL
from airflow.operators.python_operator import PythonOperator
from config import default_args
from uni_etl import extract, transform, load_s3

#Configuración basica de logs para el dag y las tasks
logging.basicConfig(
    format= '%(asctime)s - %(name)s - %(message)s',
    level= logging.INFO,
    datefmt='%Y-%m-%d'
)
#Logger listo para logear eventos
logger = logging.getLogger('Dag-Universidades')
with DAG(
    'etl-ot282-g',
    default_args=default_args,
    description='DAG para hacer un ETL de Universidades',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 8, 29),
    catchup=False,
    tags=['ETL'],
) as dag:

    logger.info('Inciando las tareas')

    #Primera task del dag extrae los datos
    extrae = PythonOperator(
        task_id='Extrae',
        python_callable=extract,
        dag=dag
    )

    #Segunda tasks del dag transforma datos con pandas - Procesa los datos obtenidos
    #Task Kennedy
    transforma_kennedy = PythonOperator(
        task_id='Transforma-Kennedy',
        python_callable=transform,
        op_args=['kennedy'],
        dag=dag
    )
    #Task Sociales
    transforma_sociales = PythonOperator(
        task_id='Transforma-Sociales',
        python_callable=transform,
        op_args=['sociales'],
        dag=dag
    )

    #Tercera task del dag carga la info lista en un S3 de AWS
    #Carga kennedy
    carga_kennedy = PythonOperator(
        task_id='carga_en_S3_kennedy',
        python_callable=load_s3,
        op_kwargs={
        'filename': './files/kennedy.txt',
        'key': 'kennedy.txt',
        'bucket_name': 'cohorte-agosto-38d749a7',
        'facultad': 'kennedy'
        },
        dag=dag
    )
    #Carga Sociales
    carga_sociales = PythonOperator(
        task_id='carga_en_S3_sociales',
        python_callable=load_s3,
        op_kwargs={
        'filename': './files/sociales.txt',
        'key': 'sociales.txt',
        'bucket_name': 'cohorte-agosto-38d749a7',
        'facultad': 'sociales'
        },
        dag=dag
    )
    #Flujo de tasks
    extrae >> transforma_kennedy >> transforma_sociales >> carga_kennedy >> carga_sociales
