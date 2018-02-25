import sys
import fileinput
import logging
import os
import time
import gzip, io

suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']


class FileReader:

    def __init__(self, input_file):

        if not os.path.exists(input_file):
            raise FileNotFoundError

        if input_file == '-':
            input = sys.stdin
        else:
            # input = io.BufferedReader(gzip.open(input_file))
            input = fileinput.hook_compressed(input_file, "r")

        self.__input = input
        pass

    def execute(self, jobs_queue, spool_length):
        logging.info("Starting reading file...")

        id = 0
        size = 0

        start = time.time()
        counter = Counter(10)
        for line in self.__input:

            id += 1
            job = (id, line)
            size += len(line)

            jobs_queue.put(job)

            if id % 1000 == 0:
                counter.count(1000, size, time.time() - start)

                size = 0
                start = time.time()

                logging.info(counter.get_info("Queue: {}, Out: {}".format(jobs_queue.qsize(), spool_length())))

        self.close()
        logging.info("Reading file finished.")

    def close(self):
        self.__input.close()


class Counter:
    def __init__(self, buffer_size):
        self.__total_count = 0
        self.__total_size = 0
        self.__total_time = 0

        self.__running_count = [0]
        self.__running_size = [0]
        self.__running_timer = [0]

        self.buffer_size = buffer_size

    def __rotate(self, l, n):
        return l[n:] + l[:n]

    def __normalize_metrics(self):
        while len(self.__running_count) > self.buffer_size:
            self.__running_count.pop(0)

        while len(self.__running_size) > self.buffer_size:
            self.__running_size.pop(0)

        while len(self.__running_timer) > self.buffer_size:
            self.__running_timer.pop(0)

    def count(self, count, size, elapsed):
        self.__total_count += count
        self.__total_size += size
        self.__total_time += elapsed

        self.__running_count.append(count)
        self.__running_size.append(size)
        self.__running_timer.append(elapsed)

        self.__normalize_metrics()

    def get_info(self, add_string):
        template = '{:6.2f}s. Total: {} items, {}. Avg speed: {:6.2f} items and {}. {}'

        count = sum(self.__running_count)
        size = sum(self.__running_size)
        timing = sum(self.__running_timer)
        return template.format(self.__total_time, self.__total_count, self.humansize(self.__total_size),
                               count / timing, self.humansize(size / timing), add_string or "")

    def humansize(self, nbytes):
        i = 0
        while nbytes >= 1024 and i < len(suffixes) - 1:
            nbytes /= 1024.
            i += 1
        f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
        return '%s %s' % (f, suffixes[i])
