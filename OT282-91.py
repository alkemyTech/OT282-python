from airflow import DAG
import dagfactory

dag_factory = dagfactory.DagFactory("/home/augusto/airflow/dags/OT282-91.yml")

dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())