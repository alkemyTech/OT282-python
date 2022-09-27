'''
Aca testearemos una parte del codigo de map reduce la cual se encarga de
mostrar top 10 palabras mas nombradas en los post por tag.
Lo que haremos a continuación es realizar test unitarios para probar algunas funcionalidades
del codigo, como por ejemplo, si retorna bien las list/dict, que no esten vacias, etc.
'''
import unittest
import xml.etree.ElementTree as ET
from opt_map_reduce_2 import mapper_tags, reduce_tags, chunkify

class TestMapReduce2(unittest.TestCase):
    '''
    Clase para realizar pruebas en el codigo de map reduce

    Args:
        unittest (_type_): libreria de la que hereda la clase TestMapReduce2
    '''
    #Datos para testear
    URL = '/home/carlosdev/Descargas/Stackoverflow/'+\
        'Stack Overflow 11-2010/112010 Meta Stack Overflow/posts.xml'
    tree = ET.parse(URL)
    root = tree.getroot()
    data_chunk = chunkify(root, 50)
    dict_mapper = mapper_tags(data_chunk)
    dict_reduce = reduce_tags(dict_mapper)
    #Este metodo verifica que el dict devuelto por mapper_tags no este vacio
    def test_dict_no_vacio_mapper_tags(self):
        '''
        chequea que el dict retornado no este vacio.
        '''
        self.assertNotIn(len(self.dict_mapper), {})
    #Este metodo verifica que el dict devuelto por reduce_tags no este vacio
    def test_dict_no_vacio_reduce(self):
        '''
        chequea que el dict retornado no este vacio.
        '''
        self.assertNotIn(len(self.dict_reduce), {})
    #Este metodo verifica que el dict devuelto por reduce_tags tenga para cada key una
    #longitud de 10
    def test_longitud_reduce(self):
        '''
        Chequea los tags del dict tiene que ser = 10
        '''
        self.assertEqual(len(self.dict_reduce['discussion']), 10)

if __name__ == '__main__':
    unittest.main()
