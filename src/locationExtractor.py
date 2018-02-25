import logging
import time
from queue import Queue
from threading import Thread

from databaseWriter import DatabaseWriter, DatabaseConfig
from filereader import FileReader
from jsonProcessor import JsonProcessor

config = DatabaseConfig(host="localhost", database="articleLocation", table="\"ArticleLocation\"", user="postgres",
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

    file_reader = FileReader(input_file)
    database_writer = DatabaseWriter(config, buffer_size=1000)
    database_writer.check_connection()

    workers = []
    for i in range(workers_count):
        worker = JsonProcessor(i)
        extractor = Thread(target=worker.execute,
                           args=(jobs_queue, output_queue))
        extractor.daemon = True  # only live while parent process lives
        extractor.start()

        worker.process = extractor
        workers.append(worker)

    output = Thread(target=database_writer.execute,
                    args=(output_queue,))
    output.start()

    output_queue_size = lambda: output_queue.qsize()
    # map job that sorts and prints output
    map = Thread(target=file_reader.execute,
                 args=(jobs_queue, output_queue_size))
    map.start()

    map.join()
    output.join()

    for _ in workers:
        jobs_queue.put(None)

    for w in workers:
        w.process.join()


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
