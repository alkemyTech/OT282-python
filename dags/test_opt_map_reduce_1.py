'''
Aca testearemos una parte del codigo de map reduce la cual se encarga de
mostrar la los 10 post mas vistos.
Lo que haremos a continuación es realizar test unitarios para probar algunas funcionalidades
del codigo, como por ejemplo, si retorna bien las listas, que no esten vacias, etc.
'''
import unittest
import xml.etree.ElementTree as ET
from opt_map_reduce_1 import mapper_id, mapper_view

class TestMapReduce1(unittest.TestCase):
    '''
    Clase para realizar pruebas en el codigo de map reduce

    Args:
        unittest (_type_): libreria de la que hereda la clase TestMapReduce1
    '''
    #Datos para testear
    URL = '/home/carlosdev/Descargas/Stackoverflow/'+\
        'Stack Overflow 11-2010/112010 Meta Stack Overflow/posts.xml'
    tree = ET.parse(URL)
    root = tree.getroot()
    #Este metodo verifica que la lista devuelta por mapper_id no este vacia
    def test_listaid_no_vacia(self):
        '''
        chequea que la lista retornada no este vacia.
        '''
        self.assertNotIn(mapper_id(self.root), [])
    #Este metodo verifica que la lista devuelta por mapper_view no este vacia
    def test_listaview_no_vacia(self):
        '''
        chequea que la lista retornada no este vacia.
        '''
        self.assertNotIn(mapper_view(self.root), [])
    #Este metodo verifica que la longitud de la lista sea igual que el root
    #que es el que contiene cada post.
    def test_igual_longitud_id(self):
        '''
        Misma longitud root que lista devuelta por mapper
        '''
        self.assertEqual(len(self.root), len(mapper_id(self.root)))
    #Este metodo verifica que la longitud de la lista sea igual que el root
    #que es el que contiene cada post.
    def test_igual_longitud_view(self):
        '''
        Misma longitud root que lista devuelta por mapper
        '''
        self.assertEqual(len(self.root), len(mapper_view(self.root)))

if __name__ == '__main__':
    unittest.main()
