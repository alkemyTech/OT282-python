import logging
from datetime import timedelta

logging.basicConfig(
    format= '%(asctime)s - %(name)s - %(message)s',
    level= logging.INFO,
    datefmt='%Y-%m-%d'
)

logger = logging.getLogger('log de modificacion universidades')

default_args = {
        'owner': 'Alkemy',
        'depends_on_past': False,
        'email': ['lolivikyalma@gmail.com.ar'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
        }