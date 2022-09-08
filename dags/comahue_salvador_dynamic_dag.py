from airflow import DAG
import dagfactory

dag_factory = dagfactory.DagFactory("/opt/airflow/dags/config_files_comahue_salvador.yml")

dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())
