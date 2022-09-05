from airflow import DAG
from dagfactory import DagFactory

file_path = "/home/carlosdev/Documentos/projects/alkemy-projects/OT282-python/dags/dag-dinamico.yml"
dag_factory = DagFactory(file_path)

#Creando depedencias de tareas
dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())
