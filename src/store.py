import logging
import time
from collections import Counter

from tqdm import tqdm

from cache import Cache
from config import Config
from models import TokenCounter
from sqlite_client import SqliteClient


class Store:
    def __init__(self, worker_id: int):
        # init cache
        self.worker_id: int = worker_id
        self.config = Config()
        self.cache = Cache(cache_len=self.config.cache_max_size)
        self.word_counter = Cache(cache_len=self.config.cache_max_size / 10)
        self.db: SqliteClient = None
        self.is_empty = True

    def init_db(self):
        self.db = SqliteClient(self.worker_id)
        self.db.create_sqlite_database(None)

    def reduce_stored_counters(self):
        logging.info("reduce_stored_counters...")

        tokens: list[str] = self.db.fetch_analysed_tokens()

        with tqdm(desc=f"[{self.worker_id}]Reduce counters", total=len(tokens), unit=" counters", leave=True) as pbar:
            for token in tokens:
                pbar.update(1)

                token_window_neighbor_sum: dict[int, int] = self.db.get_token_window_neighbor_sum(token)

                token_stats: dict = {"word": token}
                for window in self.config.windows:
                    top_n_neighbors: dict[str, int] = self.db.get_top_n_neighbors(token, window)

                    for idx, (neighbor, frequency) in enumerate(top_n_neighbors.items()):
                        token_stats[f'w_{window}{idx + 1}'] = neighbor
                        token_stats[f'p_{window}{idx + 1}'] = frequency / token_window_neighbor_sum[window]
                    if len(top_n_neighbors) != window:
                        for i in range(len(top_n_neighbors), window):
                            token_stats[f'w_{window}{i + 1}'] = neighbor
                            token_stats[f'p_{window}{i + 1}'] = frequency / token_window_neighbor_sum[window]


                logging.info(token_stats)
                self.db.update_token([token_stats])


    def increment(self, word: str, window: int, neighbor: str, addition: int = 1):
        self.get_or_create(word, window, neighbor).neighbor_frequency += addition

    def increment_word_freq(self, word):
        frequency = self.word_counter.get(word)
        if frequency is None:
            self.word_counter.set(word, 1)
        else:
            self.word_counter.set(word, frequency + 1)

    # get or create
    def get_or_create(self, word: str, window: int, neighbor: str) -> TokenCounter:
        key: str = f'{word}.{window}.{neighbor}'

        token_counter: TokenCounter = self.cache.get(key)
        if token_counter is None:
            token_counter = TokenCounter(word, int(window), neighbor, neighbor_frequency=0)
            self.cache.set(key, token_counter)
        return token_counter

    def dump_overflow(self):
        overflow: dict[str, TokenCounter] = self.cache.pop_overflow()
        if overflow and len(overflow) > 0:
            start_process_time = time.time()
            self.is_empty = False
            self._append(overflow.values())
            logging.info(f"Overflow of {len(overflow)} items dumped in {time.time() - start_process_time:03f}s")

        word_overflow: dict[str, int] = self.word_counter.pop_overflow()
        if word_overflow and len(word_overflow) > 0:
            self._append_words(word_overflow)

    def _append(self, token_neighbor_counters: list[TokenCounter]):
        self.db.append_neighbor_counters(token_neighbor_counters)

    def _append_words(self, word_counter: dict[str, int]):
        self.db.append_words(word_counter)


    def close(self):
        logging.info("closing")
        self._append(self.cache.values())
        self._append_words(self.word_counter)

        logging.info(f'most common word of worker: {Counter(self.word_counter).most_common(10)}')

        logging.info("clean cache")
        self.cache.clear()
        self.reduce_stored_counters()
        logging.info("store closed")
