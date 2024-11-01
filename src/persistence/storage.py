import logging
from collections.abc import Iterable

from models import Token, dict_to_dataclass
from persistence import mongo_client
from persistence.IStorage import IStorage
from persistence.cache import Cache

token_cache = Cache(cache_len=100000)


class Storage(IStorage):
    def __init__(self):
        #init cache
        logging.info("initiate cache")

        for t in mongo_client.find_top_tokens(10000):
            try:
                token_cache[t['token']] = dict_to_dataclass(t)
            except Exception as exc:
                logging.error(f"failed to restore cache on token {t}")
                raise exc

        logging.info(f"cache filled initiated size={len(token_cache)}")


    def insert_one(self, data: Token) -> None:
        pass

    def insert_many(self, tokens: Iterable[Token]) -> None:
        mongo_client.save_tokens(tokens)

    def find_all(self) -> dict[str, Token]:
       return {t['token']: dict_to_dataclass(t) for t in mongo_client.find_all_tokens()}

    def find(self, token: str) -> Token:
        result: Token = token_cache.get(token)
        if result is not None:
            return result
        else:
            try:
                document = mongo_client.find_token(token)
                if document:
                    result = dict_to_dataclass(document)
            except Exception as e:
                logging.error(f"failed to serialize json to Token class: {e}")
                raise

        if result:
            token_cache[token] = result

        return result

    def update(self, token: Token) -> None:
        pass

    def update_many(self, tokens: Iterable[Token]) -> None:
        mongo_client.update_token_neighbors(tokens)
        # mongo_client.save_tokens(tokens)

    def save_many(self, tokens: set[Token]) -> None:
        new_tokens = set(t for t in tokens if t.id is None)
        existing_tokens = tokens - new_tokens

        if len(new_tokens) > 0:
            self.insert_many(new_tokens)
        if len(existing_tokens) > 0:
            self.update_many(existing_tokens)

        token_cache.print_cache_stats()

    def delete_all(self) -> None:
        pass
