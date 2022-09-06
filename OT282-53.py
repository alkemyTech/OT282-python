
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import logging



logging.basicConfig(filename='My_dag',level=logging.DEBUG, format='%(asctime)s - %(name)s - %(message)s',datefmt='%Y-%m-%d')

def extract():
    hook= PostgresHook(postgres_conn_id='postgres_localhost')
    conn = hook.get_conn()
    cursor= conn.cursor()

    #For Flores University
    cursor.execute("""SELECT universidad,carrera, fecha_de_inscripcion,name, sexo,fecha_nacimiento, codigo_postal,
                     correo_electronico, to_date(fecha_de_inscripcion, 'YYYY/MM/DD') 
                    AS fecha_inscripcion_date
                    FROM 
                        flores_comahue
                    WHERE 
                        universidad LIKE '%FLORES%' 
                    AND 
                        to_date(fecha_de_inscripcion, 'YYYY/MM/DD') BETWEEN '2020-09-01' AND  '2021-02-01'
                    ORDER BY 
                        fecha_inscripcion_date
                    LIMIT 5
                    """)
    
    #A little test. Creating the list.
    datos=[]
    for i in cursor:
        print(i[0])
        datos.append(i)
    print(datos)   
    
    #Writing the file
    with open ("dags/datos.csv", "w") as f:
        for i in datos:
            f.write("\n")
            for j in i:
                f.write(str(j)+",")
    

    #For Villa María University
    cursor.execute("""  SELECT 
                            universidad, carrera,fecha_de_inscripcion, nombre, sexo,fecha_nacimiento, localidad, email, fecha_de_inscripcion :: date AS fecha_inscripcion_date
                        FROM 
                            salvador_villa_maria
                        WHERE
                            universidad LIKE '%VILLA%'
                        AND 
                            fecha_de_inscripcion :: date between '2020-09-01' and  '2021-02-01'
                        ORDER BY 
                            fecha_inscripcion_date
                        LIMIT 5
                    """)
    
    #Creating the list.
    datos=[]
    for i in cursor:
        print(i[0])
        datos.append(i)
    print(datos)   
    
    #Adding the data to the file
    with open ("dags/datos.csv", "a") as f:
        for i in datos:
            f.write("\n")
            for j in i:
                f.write(str(j)+",")

    cursor.close()
    conn.close()
    logging.info("Funcó??????")


#Default DAG'a Arguments 
default_args= {
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

#Defining the DAG 
with DAG(
    dag_id="dag_base_datos_53", 
    start_date=datetime(2022,1,1),
    schedule_interval='0 * * * *',
    default_args= default_args,
    catchup=False) as dag:

        task_A = PythonOperator(
            task_id="extract",
            python_callable=extract
        )        


        task_A


