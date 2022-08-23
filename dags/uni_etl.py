'''
Este archivo ejecuta las funciones de extraer, transformar y cargar.
'''
import logging
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from config import HOST_DB, USER_DB, PASSWORD_DB, DB, facultades

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
        with open(i + '.sql', 'r', encoding='utf-8') as temp:
            sql_query = temp.read()
            crea_csv(engine, sql_query, i)
            logger.info('Universidad %s completada exitosamente', i)


def transform():
    '''
    En esta función levantamos en pandas el archivo extraido anteriormente
    y lo procesamos, limpiamos y sacamos los insigth que nos pidieron, y lo 
    dejamos listo para cargarlo en un s3 de AWS.
    '''
    #logger listo para logguear eventos
    logger = logging.getLogger('Task-Transform')
    logger.info('Transform')

def load_s3():
    '''
    En esta función se sube el archivo a un s3 de AWS.
    '''
    #logger listo para logguear eventos
    logger = logging.getLogger('Task-Load')
    logger.info('Load a un S3 de AWS')
