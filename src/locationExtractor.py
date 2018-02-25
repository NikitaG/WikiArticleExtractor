import logging
import time
from threading import Thread
from queue import Queue
from multiprocessing import Value

from filereader import FileReader
from databaseWriter import DatabaseWriter, DatabaseConfig
from jsonProcessor import JsonProcessor

config = DatabaseConfig(host="localhost", database="articleLocation", table="ArticleLocationTmp", user="postgres",
                        password="123456")

def process_dump(input_file, out_file, workers_count):
    """
        :param input_file: name of the wikipedia dump file; '-' to read from stdin
        :param out_file: directory where to store extracted data, or '-' for stdout
        :param workers_count: number of extraction processes to spawn.
        """

    logging.info("Starting map reduce processes...")

    workers_count = max(1, workers_count)
    maxsize = 10 * workers_count

    # output queue
    output_queue = Queue(maxsize=maxsize)
    # input queue
    jobs_queue = Queue(maxsize=maxsize)

    fileReader = FileReader(input_file)
    databaseWriter = DatabaseWriter(config)

    workers = []
    for i in range(workers_count):
        worker = JsonProcessor(i)
        extractor = Thread(target=worker.execute,
                            args=(jobs_queue, output_queue))
        extractor.daemon = True  # only live while parent process lives
        extractor.start()

        worker.process = extractor
        workers.append(worker)

    output = Thread(target=databaseWriter.execute,
                     args=(output_queue, ))
    output.start()

    output_queue_size = lambda: output_queue.qsize()
    # map job that sorts and prints output
    map = Thread(target=fileReader.execute,
                  args=(jobs_queue, output_queue_size, maxsize))
    map.start()


    map.join()
    output.join()

    for _ in workers:
        jobs_queue.put(None)

    for w in workers:
        w.process.join()

def main():
    createLogger(False, True)

    input_file = '../wikidata.json.bz2'
    output_path = ''
    threads = 1

    process_dump(input_file, output_path, threads)

def readFile():
    import bz2

    with bz2.BZ2File('../wikidata.json.bz2', 'rb') as f:
        i = 0
        start = time.time()
        total = 0
        while True:
            i += 1

            l = len(f.read(1024*1024))
            if l == 0: break;

            total += l

            print("{:6.2f}.".format((total//1024//1024) / (time.time() - start)))
        print(total/1024/1024/1024)





def createLogger(quiet, debug):
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)

    logger = logging.getLogger()

    if not quiet:
        logger.setLevel(logging.INFO)
    if debug:
        logger.setLevel(logging.DEBUG)

    return logger

if __name__ == '__main__':
    #readFile()
    main()
