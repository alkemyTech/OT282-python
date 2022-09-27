from operator import itemgetter
import logging
import logging.config
import xml.etree.ElementTree as ET
logging.config.fileConfig('config.cfg')
tree = ET.parse('Stack Overflow 11-2010/112010 Meta Stack Overflow/posts.xml')
root = tree.getroot()


        #Top 10 tipo de post con mayor respuestas aceptadas
        
def mapper(root):
    id = []
    accepted_answer_Id = []
    
    for info in root:
        if info.attrib['PostTypeId'] == "1":
            try:
                accepted_answer_Id.append(info.attrib['AcceptedAnswerId'])
                id.append(info.attrib['Id'])
            except:
                pass
    
    accepted_Id = [int(x) for x in accepted_answer_Id]
    dictionary = dict(zip(id, accepted_Id))
    return dictionary


def reducer(dictionary):
    accepted_answer = sorted(dictionary.items(),key=itemgetter(1), reverse=True)
    top_ten = []
    for top in range(10):
        
        top_ten.append(accepted_answer[top])
    return top_ten
 

if __name__=='__main__':
    dictionary =mapper(root)
    reducer(dictionary)
