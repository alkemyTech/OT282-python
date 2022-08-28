"""COMO: Analista de datos
QUIERO: Crear una función Python con Pandas para cada universidad
PARA: poder normalizar los datos de las mismas
Criterios de aceptación: 
Una funcion que devuelva un txt para cada una de las siguientes universidades con los datos normalizados:
Universidad De Flores
Universidad Nacional De Villa María

Datos Finales:
university: str minúsculas, sin espacios extras, ni guiones
career: str minúsculas, sin espacios extras, ni guiones
inscription_date: str %Y-%m-%d format
first_name: str minúscula y sin espacios, ni guiones
last_name: str minúscula y sin espacios, ni guiones
gender: str choice(male, female)
age: int
postal_code: str
location: str minúscula sin espacios extras, ni guiones
email: str minúsculas, sin espacios extras, ni guiones

Aclaraciones:
Para calcular codigo postal o locación se va a utilizar el .csv que se encuentra en el repo.
La edad se debe calcular en todos los casos"""


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
from datetime import datetime, timedelta, date
import logging


#--------------------------------------    START     -------------------------------------------------------------------------------------


logging.basicConfig(filename='My_dag',level=logging.DEBUG, format='%(asctime)s - %(name)s - %(message)s',datefmt='%Y-%m-%d')

def extract():
    hook= PostgresHook(postgres_conn_id='postgres_localhost')
    conn = hook.get_conn()
    cursor= conn.cursor()

    #For Flores University
    cursor.execute("""SELECT universidad as university,
                            carrera,
                            fecha_de_inscripcion,
                            name,
                            sexo,
                            fecha_nacimiento,
                            codigo_postal,
                            correo_electronico,
                            to_date(fecha_de_inscripcion, 'YYYY/MM/DD') as fecha_inscripcion_date
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
    with open ("dags/datos_Flores.csv", "w") as f:
        for i in datos:
            f.write("\n")
            for j in i:
                f.write(str(j)+",")
    
    #Working with the file
    columnas=['university','career','inscription_date','full_name','gender','age','postal_code','email','location','WTF']
    df_flores=pd.read_csv('dags/datos_Flores.csv', names=columnas, index_col=False)
    
    def calculate_age(born):
        born = datetime.strptime(born, "%Y-%m-%d").date()
        today = date.today()
        return today.year - born.year 
    
    name = df_flores["full_name"].str.split('[_, ]',expand=True)
    name.columns = ['first_name_s', 'last_name']
    
    df_flores = pd.concat([df_flores, name], axis=1)

    df_flores = df_flores.reindex(
        columns=['university','career','inscription_date','first_name_s','last_name','gender','age','postal_code',
                'email','location','WTF','full_name'
                ])

    
    def mini(car):
        car = car.lower()
        return car 

    df_flores['university'] = df_flores['university'].apply(mini)
    df_flores['career'] = df_flores['career'].apply(mini)
    df_flores['first_name_s'] = df_flores['first_name_s'].apply(mini)
    df_flores['last_name'] = df_flores['last_name'].apply(mini)
    df_flores['email'] = df_flores['email'].apply(mini)
    df_flores['age'] = df_flores['age'].apply(calculate_age)

    url = 'https://drive.google.com/file/d/1or8pr7-XRVf5dIbRblSKlRmcP0wiP9QJ/view'
    path = 'https://drive.google.com/uc?export=download&id='+url.split('/')[-2]
    cp = pd.read_csv(path)
    
    for i in range(len(df_flores.postal_code)):
        for j in range(len(cp.codigo_postal)):
            if (df_flores.postal_code[i] == cp.codigo_postal[j]):
                df_flores['location'][i]= cp.localidad[j]

    df_flores['location'] = df_flores['location'].apply(mini)

    df_flores = df_flores.reindex(columns=['university','career','inscription_date','first_name_s','last_name','gender','age','postal_code','location','email','WTF','full_name'])
    df_final_flores = df_flores.drop(columns=['WTF','full_name']) 
    df_final_flores.to_csv('dags/Flores.txt', header=True, index=True, sep=',', mode='a')

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
    with open ("dags/datos_Maria.csv", "a") as f:
        for i in datos:
            f.write("\n")
            for j in i:
                f.write(str(j)+",")

    #Working with the file
    columnas=['university','career','inscription_date','full_name','gender','age','location','email','Inscription','WTF']
    df_villa=pd.read_csv('dags/datos_Maria.csv', names=columnas, index_col=False)
    df_villa

    #Changing date of birth for age
    def calculate_age(born):
        born_str = born
        born = datetime.strptime(born, "%d-%b-%y").date()
        today = date.today()
        if (int(born_str[-2:]) > 68):
            res = today.year - born.year
        else:
            res= today.year - (born.year - 100)
        return res

    df_villa['age'] = df_villa['age'].apply(calculate_age)
    
    #Separating full name in name and last name
    name = df_villa["full_name"].str.split('[_, ]',expand=True)
    name.columns = ['first_name_s', 'last_name']
    df_villa = pd.concat([df_villa, name], axis=1)
    
    #Extracting the "_" character
    for i in range(len(df_villa.location)):
        df_villa.location[i]=str(df_villa.location[i]).replace('_',' ')

    #Importing the postal code sheet
    url = 'https://drive.google.com/file/d/1or8pr7-XRVf5dIbRblSKlRmcP0wiP9QJ/view'
    path = 'https://drive.google.com/uc?export=download&id='+url.split('/')[-2]
    cp = pd.read_csv(path)
    
    #Adding the postal code to the principal DF
    df_villa['postal_code']= 5 #si no definia la columna primero me tiraba un error
    for i in range(len(df_villa.location)):
        for j in range(len(cp.localidad)):
            if (df_villa.location[i] == cp.localidad[j]):
                df_villa['postal_code'][i]= cp.codigo_postal[j]

    #Puting in lowercase the columns
    def mini(par):
        par = par.lower()
        return par 

    df_villa['university'] = df_villa['university'].apply(mini)
    df_villa['career'] = df_villa['career'].apply(mini)
    df_villa['first_name_s'] = df_villa['first_name_s'].apply(mini)
    df_villa['last_name'] = df_villa['last_name'].apply(mini)
    df_villa['email'] = df_villa['email'].apply(mini)
    df_villa['location'] = df_villa['location'].apply(mini)
    
    #Preparing the final data (df_final_villa)
    df_villa = df_villa.reindex(columns=['university','career','inscription_date','first_name_s','last_name','gender','age','postal_code','location','email','WTF','full_name'])
    df_final_villa= df_villa.drop(columns=['WTF','full_name']) 
    df_final_villa.to_csv('dags/Maria.txt', header=True, index=True, sep=',', mode='a')


    #Closing the cursor and onnection
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
    dag_id="dag_base_datos_61", 
    start_date=datetime(2022,1,1),
    schedule_interval='0 * * * *',
    default_args= default_args,
    catchup=False) as dag:

        task_A = PythonOperator(
            task_id="extract",
            python_callable=extract
        )        


        task_A

