import logging
import time

class JsonProcessor:
    def __init__(self, id):
        self.__id = id

        self.process = None
        pass

    def execute(self, jobs_queue, output_queue):
        logging.info("JsonProcessor #{} is starting...".format(self.__id))

        while True:
            job = jobs_queue.get()
            if job:
                jobId = job[0]
                line = job[1].decode('utf-8')

                obj = self.__convert(jobId, line)
                output_queue.put(obj)

                #tim# e.sleep(0.001)
                #logging.debug("Processing job #{} in processor #{}".format(job[0], self.__id))
                pass
            else:
                break

        logging.info("JsonProcessor #{} completed.".format(self.__id))

    def __convert(self, id, jsonString):
        return {"id": id}
