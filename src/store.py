import logging
from dataclasses import dataclass
from itertools import batched

from collections import Counter
from tqdm import tqdm
from unqlite import UnQLite

from cache import Cache
from config import Config


@dataclass
class TokenCounter:
    __slots__ = ("key", "neighbor_frequency")
    key: str  # word.window.neighbor
    neighbor_frequency: int
    # neighbor: str
    # word: str
    # window: int


class Store:
    def __init__(self, worker_id: int):
        # init cache
        self.worker_id: int = worker_id
        self.config = Config()
        self.cache = Cache(cache_len=self.config.cache_max_size)
        self.word_counter = Cache(cache_len=self.config.cache_max_size / 10)
        self.db: UnQLite = None
        self.is_empty = True

    def _init_db(self):
        self.db = UnQLite(
            filename=f'{self.config.mongo_db}/{self.config.mongo_collection}.{self.worker_id}.udb',
            open_database=True
        )

    def reduce_stored_counters(self):
        logging.info("reduce_stored_counters...")
        progress_bar = tqdm(desc="Reduce counters", unit=" counters", leave=True)

        words = []

        with self.db.transaction():
            with self.db.cursor() as cursor:
                line = 0
                for key, value in cursor:
                    line+=1
                    if key[0:3] == "wf_":
                        continue

                    key_parts = key.split(".")

                    if len(key_parts) == 1:
                        try:
                            words.append(key)
                            self.db[key] = sum(int(i) for i in value.decode().split(",") if len(i) > 0)
                        except ValueError:
                            if key != 'dc_neighbors':
                                logging.error("wtf %s",key,  value)
                    else:
                        freq = sum(int(i) for i in value.decode().split(",") if len(i) > 0)
                        # store_batch.append({
                        #     'key': key,
                        #     'word': key_parts[0],
                        #     'window': int(key_parts[1]),
                        #     'neighbor': key_parts[2],
                        #     'freq': freq
                        # })
                        try:
                            self.db.append(f'wf_{key_parts[0]}.{key_parts[1]}', f'{key_parts[2]}.{freq}')
                        except:
                            logging.info(self.db[f'wf_{key_parts[0]}.{key_parts[1]}'])
                            logging.info(f'wf_{key_parts[0]}.{key_parts[1]}')
                            logging.info(f'{key_parts[2]}:{freq};')
                            logging.info(line)
                            raise
                        # del self.db[key]
                    progress_bar.update(1)

                    # neighbors.store(store_batch)

        logging.info("reduced...")
        # len(words)
        # words_collection = self.db.collection('dc_words')
        # words_collection.create()
        #
        # for word in words:
        #     for window in self.config.windows:
        #         # counters = neighbors.filter(lambda counter: counter['word'] == word and counter['window'] == window)
        #         counter = Counter({c['neighbor']: c['freq'] for c in counters})
        #         del counter['redacted']
        #         top_5 = counter.most_common(5)
        #         logging.info(f'{word}.{window}: {top_5}')





    def close(self):
        self._append(self.cache.values())
        self._append_words(self.word_counter.items())
        self.cache.clear()
        self.reduce_stored_counters()
        logging.info("closing")
        self.db.close()

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
        if self.db is None:
            self._init_db()

        key: str = f'{word}.{window}.{neighbor}'

        token_counter: TokenCounter = self.cache.get(key)
        if token_counter is None:
            token_counter = TokenCounter(key=key, neighbor_frequency=0)
            self.cache.set(key, token_counter)
        return token_counter

    def dump_overflow(self):
        overflow: dict[str, TokenCounter] = self.cache.pop_overflow()
        if overflow and len(overflow) > 0:
            self.is_empty = False
            self._append(overflow.values())
            logging.info(f"Overflow of {len(overflow)} items dumped")

        word_overflow: dict[str, int] = self.word_counter.pop_overflow()
        if word_overflow and len(word_overflow) > 0:
            self._append_words(word_overflow)

    def _append(self, token_counters: list[TokenCounter]):
        with self.db.transaction():
            for token_counter in token_counters:
                self.db.append(token_counter.key, "," + str(token_counter.neighbor_frequency))

    def _append_words(self, word_counter_v: dict[str, int]):
        with self.db.transaction():
            for word, frequency in word_counter_v:
                self.db.append(word, "," + str(frequency))
