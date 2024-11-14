import logging
import os
import time
from multiprocessing import Process
from faster_fifo import Queue

from analyzer.ngram_analyzer import analyze_word
from counter_db import CounterDB, config
from models import NewToken
from persistence.storage import Storage


class Worker(Process):
    def __init__(self, worker_id: int, new_token_queue: Queue):
        Process.__init__(self)

        self.worker_id = worker_id
        self.queue = new_token_queue
        self.active_time = 0  # Time spent actively processing tasks
        self.start_time = time.time()  # Start time for the worker
        self.daemon = True  # Ensures workers terminate with main program
        self.store: Storage = None
        self.error_counter = 0
        self.counter = 0

    def run(self):
        logging.info(f'Worker[id={self.worker_id}] process started on pid[{os.getpid()}]')
        self.store = Storage()
        self.start_time = time.time()

        while True:
            sublist = self.queue.get(timeout=60_000)

            self.counter += 1

            if sublist is None:
                self.print_info()
                self.dump_cache()
                self.close()
                break  # Exit signal received

            start_process_time = time.time()

            try:
                token = self.get_or_create(sublist)
                analyze_word(token)
                if self.counter % config.system_counter_dump_check_interval == 1:
                    self.dump_small_counters()

            except Exception as ex:
                logging.error(f"Failed to process token {sublist} with error", ex)
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

    def get_or_create(self, sublist: list[str]):
        word = sublist[max(config.windows)]

        token: NewToken = self.store.find_shallow(word)
        if token is None:
            token = NewToken(word=word, word_bag=sublist, stats={window: CounterDB() for window in config.windows})
            self.store.save_many({token})
        else:
            token.word_bag = sublist
        return token

    def utilization(self):
        """Calculates the utilization as the ratio of active time to total time."""
        total_time = time.time() - self.start_time
        return (self.active_time / total_time) * 100 if total_time > 0 else 0

    def close(self):
        self.store.token_cache.clear()

    def print_info(self):
        utilization = self.utilization()
        logging.info(f"Worker {self.worker_id}: Utilization = {utilization:.2f}%, cache: {self.store.get_cache_stats()}")

    def dump_cache(self):
        self.store.save_cache()

    def dump_small_counters(self):
        self.store.trim_cache()
        for new_token in self.store.token_cache.values():
            new_token.cleanup_counters()

