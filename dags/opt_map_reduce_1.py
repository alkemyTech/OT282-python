'''
Optimizando map reduce anterior
### *Top 10 post mas vistos*
'''
import xml.etree.ElementTree as ET
from multiprocessing import Pool
from datetime import datetime

def chunkify(arre, chunk_size: int):
    '''
    Función que dividie el iterable

    Args:
        arre (_type_): iterable a dividir
        chunk_size (int): numero entero por el cual se va a dividir el iterable
    '''
    for i in range(0, len(arre), chunk_size):
        yield arre[i:i+chunk_size]

#Mapper lista posts id
def mapper_id(data_chunky) -> list:
    '''
    Función que mapea y devuelve una lista de pares id del post y cantidad de vistas

    Args:
        root (generador): Elementos extraidos desde el xml

    Returns:
        list: guarda la info
    '''
    #itero sobre el elemento que contiene todos los post del .xml
    arre_id = []
    for data in data_chunky:
        arre_id.append(data.attrib['Id'])
        #yield i.attrib['Id']
    return arre_id

#Mapper lista viewcount
def mapper_view(data_chunky) -> list:
    '''
    Función que mapea y devuelve una lista de pares id del post y cantidad de vistas

    Args:
        root (generador): Elementos extraidos desde el xml

    Returns:
        list: guarda la info
    '''
    #itero sobre el elemento que contiene todos los post del .xml
    arre_view = []
    for data in data_chunky:
        arre_view.append(data.attrib['ViewCount'])
        #yield i.attrib['ViewCount']
    return arre_view

#Reduce
def reducer(posts_list: list) -> None:
    '''
    Función que muestra los 10 posts mas vistos

    Args:
        posts (list): lista de pares id_post y viewcount
    '''
    #ordeno la lista
    post_view2 = sorted(posts_list, key=lambda x: int(x[1]), reverse=True)
    #muestro los primeros 10
    print('ID Post - ViewCount')
    print('--'*10)
    for i in range(10):
        print(f'{post_view2[i][0]}       {post_view2[i][1]}')
        print('--'*10)

#Flatten
def to_flatten(lista):
    '''
    Pasar una lista de lista a una lista
    
    Args:
        lista (_type_): lista de lista

    Returns:
        _type_: una lista flatten
    '''
    return [item for i in lista for item in i]

if __name__ == '__main__':
    URL = '/home/carlosdev/Descargas/Stackoverflow/'+\
        'Stack Overflow 11-2010/112010 Meta Stack Overflow/posts.xml'
    tree = ET.parse(URL)
    root = tree.getroot()
    data_chunk_id = chunkify(root, 100)
    data_chunk_view = chunkify(root, 100)
    ahora = datetime.now()
    with Pool(4) as p:
        posts_id = p.map(mapper_id, data_chunk_id)
        posts_view = p.map(mapper_view,data_chunk_view)
    posts_id = to_flatten(posts_id)
    posts_view = to_flatten(posts_view)
    posts = list(zip(posts_id, posts_view))
    reducer(posts)
    ya = datetime.now()-ahora
    print(f'Cantidad de Segundo de demora: menos de {ya.seconds} sec')
