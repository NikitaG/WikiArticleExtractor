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

    def execute(self, jobs_queue, spool_length, max_spool_length):
        logging.info("Starting reading file...")

        id = 0
        start = time.time()
        totalSize = 0
        for line in self.__input:

            id += 1
            # delay = 0
            # if spool_length() > max_spool_length:
            #     # reduce to 10%
            #     while spool_length() > max_spool_length / 10:
            #         time.sleep(10)
            #         delay += 10
            # if delay:
            #     logging.debug('Delay %ds', delay)

            job = (id, line)
            totalSize += len(line)
            s = time.time()
            jobs_queue.put(job)
            e = time.time()
            if e - s > 0.01:
                print(e-s)
            #d.append(job)

            if id % 1000 == 0:

                elapsed = time.time() - start
                logging.info('{:6.2f}s. Total: {} items, {}. Avg speed: {:6.2f} items and {}. Queue: {}, Out: {}'.format( elapsed, id, self.humansize(totalSize), id / elapsed, self.humansize(totalSize / elapsed), jobs_queue.qsize(), spool_length() ))

            #logging.debug(line)

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