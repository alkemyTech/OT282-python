import logging

logging.basicConfig(filename='app.log',
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d',
                    level=logging.DEBUG)
