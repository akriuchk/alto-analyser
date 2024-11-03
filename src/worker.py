import logging
import os
import time
from multiprocessing import Process, Queue

from analyzer.ngram_analyzer import analyze_word
from models import NewToken
from persistence.storage import Storage


class Worker(Process):
    def __init__(self, worker_id, queue: Queue):
        Process.__init__(self)

        self.worker_id = worker_id
        self.queue = queue
        # self.cache = Cache(cache_len=Config().cache_max_size)
        self.active_time = 0  # Time spent actively processing tasks
        self.start_time = time.time()  # Start time for the worker
        self.daemon = True  # Ensures workers terminate with main program
        self.store: Storage = None
        self.error_counter = 0

    def run(self):
        logging.info(f'Worker[id={self.worker_id}] process started on pid[{os.getpid()}]')
        self.store = Storage()
        self.start_time = time.time()

        while True:
            new_token = self.queue.get()
            if new_token is None:
                self.dump_cache()
                self.print_info()
                self.close()
                break  # Exit signal received

            start_process_time = time.time()

            try:
                token = self.get_or_create(new_token)
                analyze_word(token)
            except Exception as ex:
                logging.error(f"Failed to process token {new_token} with error {ex}", ex)
                self.error_counter +=1

                if self.error_counter <= 10:
                    continue
                else:
                    self.dump_cache()
                    self.print_info()
                    self.close()
                    raise ex
            finally:
                # Track active processing time
                end_process_time = time.time()
                self.active_time += end_process_time - start_process_time

    def get_or_create(self, new_token):
        token: NewToken = self.store.find(new_token.word)
        if token is None:
            token = new_token
            self.store.save_many({token})
        else:
            token.word_bag = new_token.word_bag
        return token

    def utilization(self):
        """Calculates the utilization as the ratio of active time to total time."""
        total_time = time.time() - self.start_time
        return (self.active_time / total_time) * 100 if total_time > 0 else 0

    def close(self):
        """Closes the MongoDB connection when the worker is finished."""
        # self.cache.close()

    def print_info(self):
        utilization = self.utilization()
        logging.info(f"Worker {self.worker_id}: Utilization = {utilization:.2f}%, cache: {self.store.get_cache_stats()}")

    def dump_cache(self):
        self.store.save_cache()


