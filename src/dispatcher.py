from hashlib import sha256
from faster_fifo import Queue

from tqdm import tqdm
from config import Config


def hash_word(word):
    return int(sha256(word.encode('utf-8')).hexdigest(), 16)


class Dispatcher:
    def __init__(self, num_workers):
        self.progress_bar = None
        config = Config()
        self.queues: list[Queue] = [Queue(maxsize=config.system_queue_size) for _ in range(num_workers)]
        self.num_workers = num_workers
        self.total_words = 0

    def dispatch(self, word: str, padded_max_window_word_sublist: list[str]):
        if self.progress_bar is None:
            # Initialize tqdm progress bar on first word dispatch
            self.progress_bar = tqdm(desc="Dispatching tokens", unit=" tokens", mininterval=8)

        worker_id = hash_word(word) % self.num_workers
        target_queue = self.queues[worker_id]

        # if worker_id in {0, 1, 2}:
            # logging.info(f"worker has been stopped for retest {worker_id}")
            # return
        # Wait to add to queue if it's full
        target_queue.put(padded_max_window_word_sublist, block=True, timeout=60_000)
        self.total_words += 1
        self.progress_bar.update(1)  # Update the word processing progress

    def get_queue(self, worker_id):
        return self.queues[worker_id]

    def shutdown(self):
        for q in self.queues:
            q.put(None)  # Signal for workers to stop
        if self.progress_bar is not None:
            self.progress_bar.close()  # Close the progress bar at shutdown
