from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

#from airflow.providers.postgres.operators.postgres import PostgresOperator
#from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
from datetime import datetime, timedelta, date
import logging
from decouple import config
from sqlalchemy import create_engine


#--------------------------------------    START     -------------------------------------------------------------------------------------


logging.basicConfig(filename='My_dag',level=logging.DEBUG, format='%(asctime)s - %(name)s - %(message)s',datefmt='%Y-%m-%d')

def extract():
       
    user_name = config('USER_NAME')
    password = config('PASSWORD')
    host = config('HOST')
    server_ = config('PORT')
    database = config('DB_NAME')
    
    engine = create_engine(f'postgresql+psycopg2://{user_name}:{password}@{host}:{server_}/{database}')

    #For Flores University

    df_flores= pd.read_sql_query("Univeridad_de_Flores", engine)
    df_flores.to_csv('DAGS/datos_Flores')
    
    #For Villa Maria University

    df_maria= pd.read_sql_query("Univeridad_de_Flores", engine)
    df_flores.to_csv('DAGS/datos_Flores')

def transform():

    #Working with the file
    columnas=['university','career','inscription_date','full_name','gender','age','postal_code','email','location','WTF']
    df_flores=pd.read_csv('dags/datos_Flores.csv', names=columnas, index_col=False)
    
    def calculate_age(born):
        born = datetime.strptime(born, "%Y-%m-%d").date()
        today = date.today()
        return today.year - born.year 
    
    
    pat = r'(MRS. |MR. )\.?'
    df_flores['full_name'].replace(pat,'',regex=True, inplace=True)
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



#Default DAG'a Arguments 

#Defining the DAG 

default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}
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
        
        Task_B = PythonOperator(
            task_id="transform",
            python_callable=transform
        ) 

        task_A >> Task_B