'''
Este archivo ejecuta las funciones de extraer, transformar y cargar.
'''

def extract():
    '''
    En esta función definimos la extraccion desde la base de datos
    en AWS, de las universidades requeridas. Usaremos sentencias sql
    para obtener los datos.
    '''
    print('Extract')

def transform():
    '''
    En esta función levantamos en pandas el archivo extraido anteriormente
    y lo procesamos, limpiamos y sacamos los insigth que nos pidieron, y lo 
    dejamos listo para cargarlo en un s3 de AWS.
    '''
    print('Transfor')

def load_s3():
    '''
    En esta función se sube el archivo a un s3 de AWS.
    '''
    print('Load a un S3 de AWS')
