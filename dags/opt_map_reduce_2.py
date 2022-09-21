'''
Optimizando map reduce anterior
### *Top 10 palabras mas nombradas en los post por tag*
'''
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from multiprocessing import Pool

def chunkify(arre, chunk_size: int):
    '''
    Función que dividie el iterable

    Args:
        arre (_type_): iterable a dividir
        chunk_size (int): numero entero por el cual se va a dividir el iterable
    '''
    for i in range(0, len(arre), chunk_size):
        yield arre[i:i+chunk_size]

#mapper
def mapper_tags(root) -> dict:
    '''
    Función que coloca todas las palabras en un unico arreglo/lista para cada key o tag

    Args:
        root (ET): el xml extraido

    Returns:
        dict: diccionario con todas la palabras por key o tag
    '''
    def normaliza(body: str) -> str:
        '''
        Función para limpiar el texto de caracteres especiales y espacios

        Args:
            body (str): texto para limpiar y appendear

        Returns:
            str: texto limpio
        '''
        temp = body
        temp = temp.split('<a')[0]
        temp = temp.replace('<p>', '').replace('</p>', '').replace(',', '').replace('?', '').replace('/', '')\
            .replace('--', '').replace('"','').replace('\n', '').replace('<br />', '').replace('(', '').replace(')', '')\
                .replace('-', '').replace("'", "").replace('.', '').replace('  ', ' ').replace('<em>', '').replace('<br>', '')\
                    .replace('<strong>', '').replace('<code>', '').replace('<blockquote>', '').replace(':', '').replace('<ul>', '')\
                        .replace('<li>', '').replace(';', '').replace('>', '')
        temp = temp.lower()
        return temp.strip()

    dic_posts = {}#diccionario para guardar posts por tags

    for data in list(root):
        for i in data:
            try:
                #extrayendo cada tag
                temp = i.attrib['Tags'].split('>')
                for j in temp:
                    j = j.replace('<', '')
                    j = j.strip()
                    j = j.lower()
                    if j != '':
                        #aca me fijo si esta el tag le agrego el body, sino creo el tag y le agrego el body
                        if j in dic_posts.keys():
                            dic_posts[j].append(normaliza(i.attrib['Body']))
                        else:
                            dic_posts[j] = []
                            dic_posts[j].append(normaliza(i.attrib['Body']))
            except:
                pass
    
    dict_palabras = {}
    for key in dic_posts.keys():
        #recorro cada key o tag
        palabras = []
        for k in dic_posts[key]:
            #recorro la lista de palabras para cada tag y creo un dict con un key unico y todas las palabras 
            # de esa key como value
            temp = k.split()
            for j in temp:
                palabras.append(j)
        dict_palabras[key] = palabras
    return dict_palabras

def reduce_tags(dict_palabra: dict) -> dict:
    '''
    Función reduce recibo un diccionario con todas las palabras de cada tags y muestro
    como salida un dict con las top 10 palabras para cada tag

    Args:
        dict_palabra (dict): diccionario con una lista de palabras por tags

    Returns:
        dict: diccionario con top 10 palabras mas repetidas por tag(key)
    '''
    dict_tags = {}
    for key in dict_palabra.keys():#recorro cada key o tag
        diccionario = {}
        for i in dict_palabra[key]:
            #recorro para el tag o key cada palabra si esta la sumo y sino la agrego a un dict nuevo
            if i in diccionario.keys():
                diccionario[i] += 1
            else:
                diccionario[i] = 1
        dict_ordenado = sorted(diccionario.items(), key=lambda x: x[1], reverse=True)#ordeno el dict lo 
        top_dict = [dict_ordenado[i] for i in range(10) if len(dict_ordenado) >= 10]#extraigo el top 10 palabras
        dict_tags[key] = top_dict#lo agrego al diccionario final 
    return dict_tags

if __name__ == '__main__':
    URL = '/home/carlosdev/Descargas/Stackoverflow/'+\
        'Stack Overflow 11-2010/112010 Meta Stack Overflow/posts.xml'
    tree = ET.parse(URL)
    root = tree.getroot()
    ahora = datetime.now()
    data_chunk = chunkify(root, 50)
    map_tag = mapper_tags(data_chunk)
    redu_tag = reduce_tags(map_tag)
    print(datetime.now()-ahora)
    print(len(redu_tag))
    for i in redu_tag['discussion']:
        print(i)