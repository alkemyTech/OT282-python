'''
Aca testearemos una parte del codigo de map reduce la cual se encarga de
mostrar Tiempo de respuesta del ranking 200-300 por score
Lo que haremos a continuación es realizar test unitarios para probar algunas funcionalidades
del codigo, como por ejemplo, si retorna bien las list/dict, que no esten vacias, etc.
'''
import unittest
import xml.etree.ElementTree as ET
from opt_map_reduce_3 import crea_listas, map_score

class TestMapReduce3(unittest.TestCase):
    '''
    Clase para realizar pruebas en el codigo de map reduce

    Args:
        unittest (_type_): libreria de la que hereda la clase TestMapReduce3
    '''
    #Datos para testear
    URL = '/home/carlosdev/Descargas/Stackoverflow/'+\
        'Stack Overflow 11-2010/112010 Meta Stack Overflow/posts.xml'
    tree = ET.parse(URL)
    root = tree.getroot()
    #Este metodo verifica que la función crear_lista no devuelva listas vacias 
    def test_no_vacio(self):
        '''
        chequea función crea_lista
        '''
        list_val1, list_val2 = crea_listas(self.root)
        self.assertNotIn(len(list_val1), [])
        self.assertNotIn(len(list_val2), [])
    #Este metodo verifica que el tipo de dato de la list devuelta sea entero
    def test_int_in_list(self):
        '''
        chequea el tipo de valor en la lista
        '''
        #genero la lista de top score de 200-300
        question, answer = crea_listas(self.root)
        top_score = []
        question_ord = sorted(question, key=lambda x: x[2], reverse=True)
        for i in range(199, 299):
            top_score.append(question_ord[i])
        list_val = map_score(top_score, answer)
        self.assertEqual(type(list_val[0]), int)

if __name__ == '__main__':
    unittest.main()
