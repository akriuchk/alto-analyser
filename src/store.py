import logging
from dataclasses import dataclass, asdict, field
from sqlitedict import SqliteDict

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
    def __init__(self, worker_id:int):
        # init cache
        self.worker_id: int = worker_id
        self.config = Config()
        self.cache = Cache(cache_len=self.config.cache_max_size)
        self.db: SqliteDict = None
        self.is_empty = True

    def _init_db(self):
        self.db = SqliteDict(
            filename=f'{self.config.mongo_db}/{self.config.mongo_collection}.{self.worker_id}.sqlite',
            tablename="tokens",
            journal_mode="OFF",
            outer_stack=True  # change to False for efficiency
        )

    def close(self):
        logging.info("closing")
        self.db.commit(blocking=True)
        self.db.close()

    def increment(self, word: str, window: int, neighbor: str, addition: int = 1):
        self.get_or_create(word, window, neighbor).neighbor_frequency += addition

    # get or create
    def get_or_create(self, word: str, window: int, neighbor: str) -> TokenCounter:
        if self.db is None:
            self._init_db()

        key: str = f'{word}.{window}.{neighbor}'
        self.db.items()

        token_counter: TokenCounter = self.cache.get(key)
        if token_counter is None:
            if not self.is_empty:
                token_counter = self.db.get(key)

            if token_counter is None:
                token_counter = TokenCounter(key=key, neighbor_frequency=0)
                logging.debug("New token %s", key)
                self.db[key] = token_counter

            self.cache.set(key, token_counter)
        return token_counter

    def dump_overflow(self):
        overflow: list[TokenCounter] = self.cache.pop_overflow()
        if overflow and len(overflow) > 0:
            self.is_empty = False
            for token_counter in overflow:
                self.db[token_counter.key] = token_counter
            logging.info(f"Overflow of {len(overflow)} items dumped")
            self.db.commit()
            logging.info("Overflow commited")


