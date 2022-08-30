from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from db_conn_csv_creation import run_university_list
from process_files import process_files
from upload_files_S3 import upload_file
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


# Variable creation. The variables were created in airflow. DATABLASE_URL should be encrypted as it handles
# sensitive information. For practical reasons the variables are included in the code as 'default_var'


DATABASE_URL = Variable.get(
    "DATABASE_URL",
    default_var="postgresql://alkymer2:Alkemy23@training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com:5432/training",
)
UNIVERSITY_LIST = Variable.get(
    "UNIVERSITY_LIST", default_var="comahue_university,delsalvador_university"
)
INPUT_PATH = Variable.get("INPUT_PATH", default_var="dags/files/")
OUTPUT_PATH = Variable.get("OUTPUT_PATH", default_var="dags/files/output/")
AWS_S3_BUCKET = Variable.get("AWS_S3_BUCKET")


with DAG(
    dag_id="comahue_delsalvador_dag",
    default_args={"owner": "alkemy"},
    schedule_interval="@hourly",
    description="This is a dag that is used to process data from two universities",
    start_date=days_ago(1),
) as dag:

    task_a = PythonOperator(
        task_id="creates_universities_csv",
        python_callable=run_university_list,
        op_kwargs={
            "database_url": DATABASE_URL,
            "university_list": UNIVERSITY_LIST,
        },
        retries=5,
    )  # This PythonOperator connects to the db, that runs comahue_sql query and stores the data in comahue.csv in the files folder
    task_b = PythonOperator(
        task_id="process_university_files",
        python_callable=process_files,
        op_kwargs={
            "input_path": INPUT_PATH,
            "output_path": OUTPUT_PATH,
        },
    )
    # This python operator process the data using pandas
    task_c = PythonOperator(
        task_id="upload_comehue_to_S3",
        python_callable=upload_file,
        op_kwargs={
            "file_name": OUTPUT_PATH + "comahue.txt",
            "bucket": AWS_S3_BUCKET,
        },
    )
    # This python operator uploads the comahue.txt file to the S3 bucket
    task_d = PythonOperator(
        task_id="upload_delsalvador_to_S3",
        python_callable=upload_file,
        op_kwargs={
            "file_name": OUTPUT_PATH + "delsalvador.txt",
            "bucket": AWS_S3_BUCKET,
        },
    )
    # This python operator uploads the delsalvador.txt file to the S3 bucket

    task_a >> task_b >> task_c
    task_b >> task_d
