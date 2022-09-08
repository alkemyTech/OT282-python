import logging
import logging.config
from os import path


logging.config.fileConfig('loggin.cfg')
logger = logging.getLogger('root')

