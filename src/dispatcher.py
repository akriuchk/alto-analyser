from hashlib import sha256
from multiprocessing import Queue

from tqdm import tqdm
from config import Config
from models import NewToken


class Dispatcher:
    def __init__(self, num_workers):
        config = Config()
        self.queues: list[Queue] = [Queue(maxsize=config.system_queue_size) for _ in range(num_workers)]
        self.num_workers = num_workers
        self.total_words = 0

    def hash_word(self, word):
        return int(sha256(word.encode('utf-8')).hexdigest(), 16)

    def dispatch(self, token: NewToken):
        if not hasattr(self, 'progress_bar'):
            # Initialize tqdm progress bar on first word dispatch
            self.progress_bar = tqdm(desc="Dispatching tokens", unit=" tokens", mininterval=8)

        worker_id = self.hash_word(token.word) % self.num_workers
        target_queue = self.queues[worker_id]

        # Wait to add to queue if it's full
        target_queue.put(token, block=True)
        self.total_words += 1
        self.progress_bar.update(1)  # Update the word processing progress

    def get_queue(self, worker_id):
        return self.queues[worker_id]

    def shutdown(self):
        for q in self.queues:
            q.put(None)  # Signal for workers to stop
        if hasattr(self, 'progress_bar'):
            self.progress_bar.close()  # Close the progress bar at shutdown
