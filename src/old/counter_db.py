from collections import Counter
from math import floor

from config import Config

config = Config()

class CounterDB(Counter):
    def __init__(self, *args, max_items: int = config.system_counter_size_limit,  **kwargs):
        self.max_items = max_items
        super().__init__(*args, **kwargs)

    def cleanup_tail(self, word, version, window) -> bool:
        if len(self) > self.max_items:
            items_to_overflow = self.most_common()[floor(self.max_items * 0.75):]
            for item, _ in items_to_overflow:
                del self[item]

            # Save overflow items to the database
            self._save_to_database(word, version, window, items_to_overflow)
            return True
        return False

    def _save_to_database(self, word, version, window, items: list[tuple[str, int]]):
        # logging.info(f"Saving [{word}] len(items)={len(items)} of counter to db")

        from old.mongo_client import increment_token_counters #circullar imports
        updates = {item[0]: item[1] for item in items}

        increment_token_counters(word, version, window, updates)
