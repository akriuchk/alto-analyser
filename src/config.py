import multiprocessing
import os
import logging
import sys


class Config:
    def __init__(self):
        self.archive_path = 'data/in/1820_1829.zip'
        self.output_path = 'data/out'
        self.decade = '1820_1829'
        self.windows = {3, 5}

        self.alto_confidence = 0.3
        self.stop_words = {'a', 'an', 'the'}
        self.filter = {'when', 'under', 'kind', 'first', 'my', 'good', 'you', 'die', 'some', 'great', 'your', 'love', 'heart', 'war'}
        self.neighbour_filter = {'redacted', 'a', 'an', 'the'}

        # self.debug_limit_lines = 260000
        self.debug_limit_lines = -1

        # self.system_workers = 1
        self.system_workers = multiprocessing.cpu_count() - 1
        # self.system_queue_size = 10
        self.system_queue_size = 1000
        # self.system_counter_dump_check_interval = 100_000
        self.system_counter_dump_check_interval = 20_000
        self.cache_max_size = 2_500_000
        # self.cache_max_size = 1_000_000
        self.cache_init_size = 10

        self.db_name = 'alto'

        os.makedirs(self.output_path, exist_ok=True)


    def to_json(self):
        return self.__dict__


DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() == 'true'

# Logger configuration
logging.basicConfig(
    level=logging.DEBUG if DEBUG_MODE else logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)-5s [%(process)d][%(filename)s:%(lineno)d] %(message)s',
    datefmt='%m/%d %H:%M:%S',
    stream=sys.stdout
)
logger = logging.getLogger('alto')
