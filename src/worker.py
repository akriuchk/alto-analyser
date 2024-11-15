import logging
import os
import time
from multiprocessing import Process

from faster_fifo import Queue

# from analyzer.ngram_analyzer import analyze_word
from config import Config
from store import Store


class Worker(Process):
    def __init__(self, worker_id: int, new_token_queue: Queue):
        Process.__init__(self)

        self.worker_id = worker_id
        self.queue = new_token_queue
        self.active_time = 0  # Time spent actively processing tasks
        self.start_time = time.time()  # Start time for the worker
        self.daemon = True  # Ensures workers terminate with main program
        self.counter = 0
        self.config = Config()
        self.store = Store(worker_id)

    def run(self):
        logging.info(f'Worker[id={self.worker_id}] process started on pid[{os.getpid()}]')
        self.start_time = time.time()

        while True:
            sublist = self.queue.get(timeout=60_000)

            self.counter += 1

            if sublist is None:
                self.print_info()
                self.close()
                break  # Exit signal received

            start_process_time = time.time()

            if self.counter % self.config.system_counter_dump_check_interval == 1:
                self.store.dump_overflow()

            try:
                self.analyze_word(sublist)
            except Exception as ex:
                logging.error(f'Failed to process token {sublist} with error', ex)
                self.print_info()
                self.close()
                raise ex
            finally:
                # Track active processing time
                end_process_time = time.time()
                self.active_time += end_process_time - start_process_time

    def analyze_word(self, sublist):
        windows = self.config.windows
        middle_idx = max(windows)
        word = sublist[middle_idx]

        self.store.increment_word_freq(word)
        if word == 'redacted':
            return


        for distance in range(1, max(windows)):

            ngram_token_before_idx = middle_idx - distance
            ngram_token_after_idx = middle_idx + distance

            word_before = sublist[ngram_token_before_idx]
            word_after = sublist[ngram_token_after_idx]

            for window in windows:
                if distance <= window:
                    if word_before != '<pad>':
                        self.store.increment(word, window, word_before)
                    if word_after != '<pad>':
                        self.store.increment(word, window, word_after)


    def utilization(self):
        """Calculates the utilization as the ratio of active time to total time."""
        total_time = time.time() - self.start_time
        return (self.active_time / total_time) * 100 if total_time > 0 else 0

    def close(self):
        self.store.close()
        pass

    def print_info(self):
        utilization = self.utilization()
        logging.info(f"Worker {self.worker_id}: Utilization = {utilization:.2f}%, cache: {self.store.cache.get_cache_stats()}")
