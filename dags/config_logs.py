'''
En este archivo levanto la configuraciones del log desde el .cfg
'''
import logging
import logging.config

#importando configuración desde config_logs.cfg
logging.config.fileConfig('config_logs.cfg')

#logger para consola y en un file
#esto loggea cada 7 dias, esta especificado en el .cfg
logger_time = logging.getLogger('alkemyTime')
logger_time.info('Loggeando por consola y en un file')
