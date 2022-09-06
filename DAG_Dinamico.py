from airflow import DAG
import dagfactory
import os 

dag_factory = dagfactory.DagFactory('/home/juanjure/airflow/docker/dags/DAG_Dinamico.yaml')

dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())