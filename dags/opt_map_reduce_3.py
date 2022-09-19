'''
Optimizando map reduce anterior
### *Tiempo de respuesta del ranking 200-300 por score*
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
#Genero una lista con el tiempo de respuesta expresado en minutos -> salida del map
def map_score(top_score: list, answer: dict) -> list:
    '''
    Función map que genera una lista con el timepo en minutos de respuesta entre
    la pregunta y la respuesta aceptada

    Args:
        top_score (list): lista con la preguntas ordenadas por score del 200-300
        answer (dict): diccionario que contiene como key el id de respuesta y valor hora creada

    Returns:
        list: lista expresada en minutos de tiempo de respuesta entre pregunta y respuesta aceptada
    '''
    list_time = []
    for res, hora_q, score in top_score:
        hora_a = answer[res]#extraigo la hora del dicc de respuestas
        diff = hora_a-hora_q
        diff_s = diff.total_seconds()
        minutos = divmod(diff_s, 60)[0]#diferencia en minutos
        list_time.append(int(minutos))#apendeando los minutos
    return list_time
#Genero un solo resultado de tiempo de respuesta en minutos -> salida del reduce
def reduce_score(list_time: list) -> None:
    '''
    Función reduce que muestra el resultado promedio de la lista generada anteriormente
    expresada en minutos de la diferencia entre la pregunta y respueta aceptada.

    Args:
        list_time (list): lista expresada en minutos del tiempo de respuesta
    '''
    calc = 0
    for i in list_time:
        calc += i
    print(f'Horas promedio de respuesta: {int(calc / 60 /100)} horas')
    print(f'En dias seria: {int(calc / 60 /100 /24)} dias')
def crea_listas(root):
    #creo una lista para las question y un diccionario para las respuestas
    question = []#lista con todas las preguntas
    answer = {}#diccionario con el id de respuesta como keys y la hora creada como valor
    for i in root:
        temp = int(i.attrib['PostTypeId'])
        if temp == 1:#pregunto si es question
            try:#este try va porque aveces no tiene respuesta aceptada o le falta otro atributo
                if i.attrib['AcceptedAnswerId'] != '' and i.attrib['CreationDate'] != '' and i.attrib['Score'] != '':
                    q_temp = []
                    q_temp.append(i.attrib['AcceptedAnswerId'])
                    horaQuestion = i.attrib['CreationDate'].replace('T', ' ').split('.')[0]
                    horaQuestion = datetime.strptime(horaQuestion, '%Y-%m-%d %H:%M:%S')
                    q_temp.append(horaQuestion)
                    q_temp.append(i.attrib['Score'])
                    question.append(q_temp)#lista con respuesta aceptada, hora y score
            except:
                pass
            
        else:#entro aca si es answer
            horaAnswer = i.attrib['CreationDate'].replace('T', ' ').split('.')[0]
            horaAnswer = datetime.strptime(horaAnswer, '%Y-%m-%d %H:%M:%S')
            answer[i.attrib['Id']] = horaAnswer#diccionario con key id respuesta y valor hora de creación
    return question, answer
if __name__ == '__main__':
    URL = '/home/carlosdev/Descargas/Stackoverflow/'+\
        'Stack Overflow 11-2010/112010 Meta Stack Overflow/posts.xml'
    tree = ET.parse(URL)
    root = tree.getroot()
    question, answer = crea_listas(root)
    #genero la lista de top score de 200-300
    top_score = []
    question_ord = sorted(question, key=lambda x: x[2], reverse=True)
    for i in range(199, 299):
        top_score.append(question_ord[i])
    #Llamando a reduce_score y map_score
    reduce_score(map_score(top_score, answer))
