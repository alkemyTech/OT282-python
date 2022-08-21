from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator

# from airflow.operators.python_operator import PythonOperator


with DAG(
    dag_id="comahue_delsalvador_dag",
    default_args={"owner": "alkemy"},
    schedule_interval="@hourly",
    description="This is a dag that is used to process data from two universities",
    start_date=days_ago(1),
) as dag:

    task_a = DummyOperator(
        task_id="task_a"
    )  # This dummyop will be a python operator that runs sql queries
    task_b = DummyOperator(
        task_id="task_b"
    )  # This dummyop will be a python operator that process the data using pandas
    task_c = DummyOperator(
        task_id="task_c"
    )  # This dummyop will be a python operator that uploads the files to S3

    task_a >> task_b >> task_c
