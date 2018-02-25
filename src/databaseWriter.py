import logging
import time

import psycopg2


class DatabaseWriter:
    def __init__(self, config, id = 0):
        self.__id = id
        self.__conn = psycopg2.connect(host=config.host, database=config.database, user=config.user, password=config.password)
        self.__sql = """INSERT INTO public.""" + config.table + """"(type, "wikiId", title, longitude, latitude, "precision", heritage)
	VALUES (%s, %s, %s, %s, %s, %s, %s);"""

    def execute(self, output_queue):
        logging.info("DatabaseWriter #{} is starting...".format(self.__id))

        while(True):
            item = output_queue.get()
            if item:
                self.__addObject(item)
            else:
                break

        logging.info("DatabaseWriter #{} finished.".format(self.__id))

    def __addObject(self, item):
        time.sleep(0.001)
        pass

class DatabaseConfig:
    def __init__(self, host, database, table, user, password):
        self.host = host
        self.database = database
        self.table = table
        self.user = user
        self.password = password

