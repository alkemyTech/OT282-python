'''
Configuraciones por defecto para los dags y base de datos
'''
from datetime import timedelta
from decouple import config

#Configuración por defecto de Airflow
default_args = {
        'owner': 'Alkemy',
        'depends_on_past': False,
        'email': ['alkemy@alkemy.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    }

#Configuraciones de la base de datos
HOST_DB = config('HOST_DB')
USER_DB = config('USER_DB')
PASSWORD_DB = config('PASSWORD_DB')
DB = config('DB')

#Lista con los facultades para trabajar, declararlas aca da la facilidad de
#escalar la cantidad de facultades que se pidan
facultades = ['sociales', 'kennedy']
