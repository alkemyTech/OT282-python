from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from db_conn_csv_creation import create_csv_from_sql
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

# Variable creation. The variables were created in airflow. DATABLASE_URL should be encrypted as it handles
# sensitive information. For practical reasons the variables are included in the code as 'default_var'

DATABASE_URL = Variable.get(
    "DATABASE_URL",
    default_var="postgresql://alkymer2:Alkemy23@training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com:5432/training",
)
COMAHUE_QUERY = Variable.get(
    "COMAHUE_QUERY", default_var="dags/sql/comahue_university.sql"
)
COMAHUE_CSV = Variable.get("COMAHUE_CSV", default_var="dags/files/comahue.csv")
DELSALVADOR_QUERY = Variable.get(
    "DELSALVADOR_QUERY", default_var="dags/sql/delsalvador_university.sql"
)
DELSALVADOR_CSV = Variable.get(
    "DELSALVADOR_CSV", default_var="dags/files/delsalvador.csv"
)


with DAG(
    dag_id="comahue_delsalvador_dag",
    default_args={"owner": "alkemy"},
    schedule_interval="@hourly",
    description="This is a dag that is used to process data from two universities",
    start_date=days_ago(1),
) as dag:

    task_a = PythonOperator(
        task_id="creates_comahue_csv",
        python_callable=create_csv_from_sql,
        op_kwargs={
            "database_url": DATABASE_URL,
            "sql_query_path": COMAHUE_QUERY,
            "output_csv_path": COMAHUE_CSV,
        },
    )  # This PythonOperator connects to the db, that runs comahue_sql query and stores the data in comahue.csv in the files folder
    task_b = PythonOperator(
        task_id="creates_delsalvador_csv",
        python_callable=create_csv_from_sql,
        op_kwargs={
            "database_url": DATABASE_URL,
            "sql_query_path": DELSALVADOR_QUERY,
            "output_csv_path": DELSALVADOR_CSV,
        },
    )  # This PythonOperator connects to the db, that runs delsalvador_sql query and stores the data in dalsalvador.csv in the files folder
    task_c = DummyOperator(
        task_id="task_b"
    )  # This dummyop will be a python operator that process the data using pandas
    task_d = DummyOperator(
        task_id="task_c"
    )  # This dummyop will be a python operator that uploads the files to S3

    task_a >> task_c >> task_d
    task_b >> task_c
