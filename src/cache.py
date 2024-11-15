import logging
import math
from collections import OrderedDict


class Cache(OrderedDict):
    """Dict with a limited length, ejecting LRUs as needed."""
    cache_hit: int = 0
    cache_miss: int = 0

    def __init__(self, *args, cache_len: int = 1000, **kwargs):
        logging.debug(f"new cache with len = {cache_len}")
        assert cache_len > 0
        self.cache_len = cache_len
        self.limit_reached = False

        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        if not self.limit_reached:
            if len(self) == self.cache_len:
                self.limit_reached = True
                logging.info(f"cache limit reached {self.cache_len}")
        else:
            raise ValueError("limit reached")

    def set(self, key, value):
        super().__setitem__(key, value)
        # if len(self) == self.cache_len:
        #     logging.debug(f"cache limit reached {self.cache_len}, do cleanup")

    def pop_overflow(self) -> dict:
        if len(self) >= self.cache_len:
            overflow = {}
            while len(self) > math.floor(self.cache_len * 0.75):
                key, value = super().popitem(last=False)
                overflow[key] = value
            # logging.info(f"cache overflow = {[t.word for  t in overflow]}")
            return overflow

    def __getitem__(self, key):
        val = super().__getitem__(key)
        super().move_to_end(key)

        self.__update_cache_stat(val)
        return val

    def get(self, key):
        val = super().get(key)
        if val is not None:
            super().move_to_end(key)

        self.__update_cache_stat(val)
        return val

    def __update_cache_stat(self, value):
        if value is None:
            self.cache_miss += 1
        else:
            self.cache_hit += 1

    def get_cache_stats(self) -> dict:
        try:
            return {
                'size': len(self),
                'hits': self.cache_hit,
                'hits(%)': f"{self.cache_hit * 100 / (self.cache_hit + self.cache_miss):.1f}%",
                'miss': self.cache_miss,
                'miss(%)': f"{self.cache_miss * 100 / (self.cache_hit + self.cache_miss):.1f}%"
            }
        except Exception:
            print(f"failed cache pretty print, raw: {self.cache_hit}, {self.cache_miss}, {self.cache_len}")
            return {
                'size': len(self),
                'hits': self.cache_hit,
                'miss': self.cache_miss,
            }
