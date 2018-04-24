import logging
import time

import psycopg2
import psycopg2.extras


class DatabaseWriter:
    @property
    def connection(self):
        if not self.__connection:
            config = self.__config

            logging.debug('Connecting to the PostgreSQL database...')
            self.__connection = psycopg2.connect(host=config.host, database=config.database, user=config.user,
                                                 password=config.password)

        return self.__connection

    def __init__(self, config, id=0, buffer_size=100):
        self.__id = id
        self.__config = config
        self.__connection = None

        columns = ["id", "label_en", "label_ru", "description_en", "description_ru", "subclasses", "instances"]
        # values = ["%s" for _ in columns]

        self.__sql = "INSERT INTO public.{} ({}) VALUES %s; ".format(config.table, ",".join(columns))
        self.__buffer = []
        self.__buffer_size = buffer_size
        self.__totalRecords = 0

    def check_connection(self):
        """ Connect to the PostgreSQL database server """
        try:
            # create a cursor
            cur = self.connection.cursor()

            # execute a statement
            cur.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            logging.info('PostgreSQL database version: {}'.format(db_version))

            # close the communication with the PostgreSQL
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(error)
        finally:
            pass

    def execute(self, output_queue):
        logging.info("DatabaseWriter #{} is starting...".format(self.__id))

        properties = {}
        with open('properties.json', 'w') as file:

            while True:
                item = output_queue.get()
                if item:
                    if item[0] == 'metadata':
                        self.__addObject(item[1])
                    item = None
                else:
                    break

        self.__commit_buffer()

        logging.info("DatabaseWriter #{} finished.".format(self.__id))

    def __addObject(self, obj):
        self.__buffer.append((
            self.__id_to_int(obj['id']), obj['label_en'], obj['label_ru'], obj['description_en'], obj['description_ru'],
            list(map(self.__id_to_int, obj['subclasses'])), list(map(self.__id_to_int, obj['instances']))))

        if len(self.__buffer) >= self.__buffer_size:
            self.__commit_buffer()

    def __id_to_int(self, ids):
        return int(ids[1:])

    def __commit_buffer(self):
        if not self.__buffer:
            return

        try:
            # create a new cursor
            cur = self.connection.cursor()

            psycopg2.extras.execute_values(cur, self.__sql, self.__buffer, page_size=self.__buffer_size)

            self.__totalRecords += len(self.__buffer)
            self.__buffer = []

            logging.debug("Database buffer committed. Total records: {}".format(self.__totalRecords))

            # execute the INSERT statement
            # (type, "wikiId", title, longitude, latitude, "precision", heritage)
            # cur.execute(self.__sql, )

            # commit the changes to the database
            self.connection.commit()

        except Exception as error:
            logging.error(error)
            raise error


class DatabaseConfig:
    def __init__(self, host, database, table, user, password):
        self.host = host
        self.database = database
        self.table = table
        self.user = user
        self.password = password
