'''
Este archivo ejecuta las funciones de extraer, transformar y cargar.
'''
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from airflow.hooks.S3_hook import S3Hook
from datetime import timedelta
from decouple import config
HOST_DB = 'HOST_DB'
USER_DB = 'USER_DB'
PASSWORD_DB = 'PASSWORD_DB'
DB = 'DB'
facultades = ['sociales', 'kennedy']
#Funiones adicionales
def crea_csv(engine, sql_query: str, facu: str) -> None:
    '''
    En esta función creamos un .csv a partir de una query sql, usando pandas
    sqlalchemy
    Args:
        engine (_type_): Conexión a la base de datos
        sql_query (str): Variables con la query para usarla con pandas.
    '''
    #Creamos un df con la query de sql para poder pasarlo a un .csv
    df_query = pd.read_sql_query(sql_query, con=engine)
    #Pasando a .csv
    directorio = Path('files')
    directorio.mkdir(parents=True, exist_ok=True)
    df_query.to_csv(Path('files', facu + '.csv'), index=False)
def normalizar(df_facu: pd.DataFrame, facultad: str) -> None:
    '''
    Función para normalizar los dataframe de las universidades, que consiste en
    usar minusculas, colocar el tipo de dato de cada serie o columna.
    Args:
        df (pd.DataFrame): Argumento recibido un df para normalizar
    '''
    #castear a integer la columna age
    df_facu['age'] = df_facu['age'].apply(lambda x: np.int8(x))
    #castear a str la columna postal_code
    df_facu['postal_code'] = df_facu['postal_code'].apply(lambda x: str(x))
    #colocar male or female
    df_facu['gender'] = df_facu.gender.replace({'f': 'female', 'm': 'male'})
    #minusculas a todo el dataframe
    df_facu[['university', 'career', 'inscription_date', 'first_last_name', 'email', 'location', 'postal_code']] = \
        df_facu[['university', 'career', 'inscription_date', 'first_last_name', 'email', 'location', 'postal_code']].apply(lambda x: x.str.lower())
    #quitar espacios
    df_facu[['university', 'career', 'inscription_date', 'first_last_name', 'email', 'location', 'postal_code']] = \
        df_facu[['university', 'career', 'inscription_date', 'first_last_name', 'email', 'location', 'postal_code']].apply(lambda x: x.str.strip())
    #quitar guiones
    df_facu[['university', 'career', 'first_last_name', 'email', 'location', 'postal_code']] = \
        df_facu[['university', 'career', 'first_last_name', 'email', 'location', 'postal_code']].apply(lambda x: x.str.replace('-', ' '))
    #quitar prefijos y sufijos a la columna first_last_name
    df_facu['first_last_name'] = df_facu['first_last_name'].str.replace('mr.', '').replace('mrs.', '').replace('ms.', '')\
        .replace('jr.', '').replace('jrs.', '').replace('dr.', '').replace('drs.', '').replace('phd.', '').replace('dds.', '')
    df_facu.to_csv(Path('files', facultad + '.txt'), index=False)
#Función Primera Task -> Extraer
def extract() -> None:
    '''
    En esta función definimos la extraccion desde la base de datos
    en AWS, de las universidades requeridas. Usaremos sentencias sql
    para obtener los datos.
    '''
    #logger listo para logguear eventos
    logger = logging.getLogger('Task-Extract')
    sql_query = ''
    try:
        engine = create_engine(f"postgresql://{USER_DB}:{PASSWORD_DB}@{HOST_DB}:5432/{DB}")
    except Exception:
        logger.warning('No se pudo conectar a la base de datos')
    logger.info('Conexión a la base de datos exitosa')
    #Itero por los archivos sql con las querys para armar los csv
    for i in facultades:
        with open(Path('sql',i+'.sql'), 'r', encoding='utf-8') as temp:
            sql_query = temp.read()
            crea_csv(engine, sql_query, i)
            logger.info('Universidad %s completada exitosamente', i)
#Funciones de transformación
#Transform kennedy
def transform(facultad) -> None:
    '''
    En esta función levantamos en pandas el archivo extraido anteriormente
    y lo procesamos, limpiamos y sacamos los insigth que nos pidieron, y lo
    dejamos listo para cargarlo en un s3 de AWS.
    '''
    #logger listo para logguear eventos
    logger = logging.getLogger('Task-Transform')
    logger.info('Transform %s', facultad)
    df_zip = pd.read_csv('./aux_files/codigos_postales.csv')#dataframe postal-codes
    df_zip.rename(columns= {'codigo_postal': 'postal_code', 'localidad': 'location'}, inplace=True)
    df_zip.location = df_zip.location.str.strip()
    if facultad == 'kennedy':
        df_kennedy = pd.read_csv('./files/kennedy.csv')
        df_kennedy.rename(columns={'_age': 'age'}, inplace=True)
        df_merge_k = pd.merge(df_kennedy, df_zip, how='left', on='postal_code')
        normalizar(df_merge_k, facultad)
        logger.info('Transform y creacion del archivo .txt hechos exitosamente!!!')
    else:
        df_sociales = pd.read_csv('./files/sociales.csv')
        df_sociales.rename(columns={'_age': 'age', '_location': 'location'}, inplace=True)
        df_sociales.location = df_sociales.location.str.strip()
        df_sociales.location = df_sociales.location.str.replace('-', ' ')
        df_merge_s = pd.merge(df_sociales, df_zip, how= 'left', on= 'location')
        normalizar(df_merge_s, facultad)
        logger.info('Transform y creacion del archivo .txt hechos exitosamente!!!')
def load_s3(filename: str, key: str, bucket_name: str, facultad: str) -> None:
    '''
    En esta función se sube los archivos a un s3 de AWS.
    '''
    #logger listo para logguear eventos
    logger = logging.getLogger('Task-Load')
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)
    logger.info('Archivo %s subido exitosamente', facultad)