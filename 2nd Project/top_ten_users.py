from genericpath import exists
from operator import itemgetter
import logging
import logging.config
import xml.etree.ElementTree as ET
logging.config.fileConfig('config.cfg')
tree = ET.parse('Stack Overflow 11-2010/112010 Meta Stack Overflow/posts.xml')
root = tree.getroot()

        #Top 10 de usuarios con mayor porcentaje de respuestas favoritas

def mapper(root):
    id = []
    favourite_count = []
    answer_count = []

    for info in root:
        if info.attrib['AnswerCount']:
            try:
                favourite_count.append(info.attrib['FavoriteCount'])
                answer_count.append(info.attrib['AnswerCount'])
                id.append(info.attrib['Id'])
            except:
                pass
    
    favourite_count = [float(x) for x in favourite_count]
    answer_count = [float(x) for x in answer_count]
    
    for i in len(favourite_count):
        favourite_count[i] = (favourite_count[i]*100)
        
    print(favourite_count)
    dictionary = dict(zip(id, favourite_count))

def reducer(dictionary):
    
    
    
    pass



mapper(root)