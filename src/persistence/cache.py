import logging
from collections import OrderedDict

class Cache(OrderedDict):
    """Dict with a limited length, ejecting LRUs as needed."""
    cache_hit: int = 0
    cache_miss: int = 0

    def __init__(self, *args, cache_len: int = 10, **kwargs):
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
            old_key = super().popitem(last=False)
            #log print(f"remove {old_key[0]} from cache")

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

    def print_cache_stats(self):
        print(f"cache stats: size: {len(self)} "
              f"hits: {self.cache_hit}({self.cache_hit / (self.cache_hit + self.cache_miss):.3f})"
              f",miss: {self.cache_miss}({self.cache_miss / (self.cache_hit + self.cache_miss):.3f})")