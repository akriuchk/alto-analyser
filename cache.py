from collections import OrderedDict

class Cache(OrderedDict):
    """Dict with a limited length, ejecting LRUs as needed."""

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
        else:
            old_key = super().popitem(last=False)
            #log print(f"remove {old_key[0]} from cache")

    def __getitem__(self, key):
        val = super().__getitem__(key)
        super().move_to_end(key)

        return val

    def get(self, key):
        val = super().get(key)
        if val is not None:
            super().move_to_end(key)

        return val
