import logging
import time

from multiprocessing import Queue, Process as Instance
# from threading import Thread as Instance
# from queue import Queue

from databaseMetadataWriter import DatabaseWriter, DatabaseConfig
from filereader import FileReader
from jsonProcessor import JsonProcessor
from jsonMetadataProcessor import JsonMetadataProcessor

config = DatabaseConfig(host="localhost", database="articleLocation", table="wiki_data_location", user="postgres",
                        password="123456")

json_processor_class = JsonProcessor


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

    file_reader = FileReader(input_file)
    database_writer = DatabaseWriter(config, buffer_size=1000)
    # database_writer.check_connection()

    workers = []
    for i in range(workers_count):
        worker = json_processor_class(i)
        extractor = Instance(target=worker.execute,
                           args=(jobs_queue, output_queue))
        extractor.daemon = True  # only live while parent process lives
        extractor.start()

        worker.process = extractor
        workers.append(worker)

    output = Instance(target=database_writer.execute,
                    args=(output_queue,))
    output.start()

    output_queue_size = lambda: output_queue.qsize()
    # map job that sorts and prints output
    map = Instance(target=file_reader.execute,
                 args=(jobs_queue, output_queue_size))
    map.start()

    map.join()

    logging.info("Completing workers...")
    for _ in workers:
        jobs_queue.put(None)

    for w in workers:
        w.process.join()

    logging.info("Completing database writer...")
    output_queue.put(None)
    output.join()

def main():
    createLogger(False, True)

    input_file = '../wikidata.json.gz'#'../wikidata.json.bz2'
    output_path = ''
    threads = 8

    process_dump(input_file, output_path, threads)


def readFile():
    import bz2

    with bz2.BZ2File('../wikidata.json.bz2', 'rb') as f:
        i = 0
        start = time.time()
        total = 0
        while True:
            i += 1

            l = len(f.read(1024 * 1024))
            if l == 0: break;

            total += l

            print("{:6.2f}.".format((total // 1024 // 1024) / (time.time() - start)))
        print(total / 1024 / 1024 / 1024)


def createLogger(quiet, debug):
    fh = logging.FileHandler("wikidata.log")
    fh.setLevel(logging.ERROR)

    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.DEBUG, handlers=[fh, logging.StreamHandler()])

    logger = logging.getLogger()

    if not quiet:
        logger.setLevel(logging.INFO)
    if debug:
        logger.setLevel(logging.DEBUG)

    return logger


if __name__ == '__main__':
    # readFile()
    main()
