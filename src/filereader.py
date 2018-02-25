import sys
import fileinput
import logging
import os
import time

suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']

class FileReader:

    def __init__(self, input_file):

        if not os.path.exists(input_file):
            raise FileNotFoundError

        if input_file == '-':
            input = sys.stdin
        else:
            input = fileinput.hook_compressed(input_file, "r")

        self.__input = input
        pass

    def execute(self, jobs_queue, spool_length):
        logging.info("Starting reading file...")

        id = 0
        start = time.time()
        totalSize = 0
        for line in self.__input:

            id += 1

            job = (id, line)
            totalSize += len(line)

            jobs_queue.put(job)

            if id % 1000 == 0:

                elapsed = time.time() - start
                logging.info('{:6.2f}s. Total: {} items, {}. Avg speed: {:6.2f} items and {}. Queue: {}, Out: {}'.format( elapsed, id, self.humansize(totalSize), id / elapsed, self.humansize(totalSize / elapsed), jobs_queue.qsize(), spool_length() ))


        self.close()
        logging.info("Reading file finished.")

    def close(self):
        self.__input.close()

    def humansize(self, nbytes):
        i = 0
        while nbytes >= 1024 and i < len(suffixes) - 1:
            nbytes /= 1024.
            i += 1
        f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
        return '%s %s' % (f, suffixes[i])