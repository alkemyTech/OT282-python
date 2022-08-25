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