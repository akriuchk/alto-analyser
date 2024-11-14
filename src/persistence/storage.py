import logging
from collections.abc import Iterable
from itertools import batched

from tqdm import tqdm

from config import Config
from models import Token, dict_to_dataclass, NewToken, dict_to_shallow_dataclass
from persistence import mongo_client
from persistence.IStorage import IStorage
from persistence.cache import Cache


class Storage(IStorage):
    def __init__(self, cache: bool = True):
        # init cache
        self.config = Config()

        if cache:
            logging.info(f"init storage with cache_len={self.config.cache_max_size}")
            self.token_cache: Cache[str, NewToken] = Cache(cache_len=self.config.cache_max_size)

            for t in mongo_client.find_top_tokens(self.config.cache_init_size):
                try:
                    self.token_cache[t['word']] = dict_to_dataclass(t)
                except Exception as exc:
                    logging.error(f"failed to restore cache on token {t}")
                    raise exc

            logging.info(f"cache initiated with size={len(self.token_cache)}")

    def insert_one(self, data: Token) -> None:
        pass

    def insert_many(self, tokens: Iterable[NewToken]) -> None:
        mongo_client.save_tokens(tokens)

    def find_all(self) -> dict[str, NewToken]:
        return {t['token']: dict_to_dataclass(t) for t in mongo_client.find_all_tokens()}

    def stream_all(self):
        return mongo_client.find_all_tokens()

    def count_all(self) -> int:
        return mongo_client.count_all_tokens()

    def find_shallow(self, word: str) -> NewToken:
        result: NewToken = self.token_cache.get(word)
        if result is not None:
            return result
        else:
            try:
                document = mongo_client.find_token(word)
                if document:
                    result = dict_to_shallow_dataclass(document)
            except Exception as e:
                logging.error(f"failed to serialize json to Token class: {e}")
                raise

        if result:
            self.token_cache.set(word, result)

        return result

    def update(self, token: Token) -> None:
        pass

    def update_many(self, tokens: Iterable[NewToken]) -> None:
        mongo_client.update_tokens_simple(tokens)
        # mongo_client.save_tokens(tokens)

    def save_many(self, tokens: set[NewToken]) -> None:
        new_tokens = set(t for t in tokens if t.id is None)
        existing_tokens = tokens - new_tokens

        if len(new_tokens) > 0:
            self.insert_many(new_tokens)
        if len(existing_tokens) > 0:
            self.update_many(existing_tokens)

        # self.token_cache.print_cache_stats()

    def delete_all(self) -> None:
        pass

    def trim_cache(self):
        overflow_items = self.token_cache.pop_overflow()
        if overflow_items and len(overflow_items) > 0:
            mongo_client.update_token_counters(overflow_items)

    def get_cache_stats(self) -> dict:
        return self.token_cache.get_cache_stats()

    def save_cache(self):
        logging.info("Save cache in batches")

        progress_bar = tqdm(desc="Cache dump to db", total=len(self.token_cache),  unit=" tokens", mininterval=5)
        for batch in batched(self.token_cache.values(), 5000):
            mongo_client.update_token_counters(batch)
            progress_bar.update(5000)
