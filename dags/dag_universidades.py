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
    'universidades',
    default_args=default_args,
    description='DAG para hacer un ETL de Universidades',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 8, 19),
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

    #Segunda task del dag transforma datos con pandas - Procesa los datos obtenidos
    transforma = PythonOperator(
        task_id='Transforma',
        python_callable=transform,
        dag=dag
    )

    #Tercera task del dag carga la info lista en un S3 de AWS
    carga = PythonOperator(
        task_id='carga_en_S3',
        python_callable=load_s3,
        dag=dag
    )

    #Flujo de tasks
    extrae >> transforma >> carga
