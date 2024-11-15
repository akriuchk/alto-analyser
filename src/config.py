import multiprocessing
import os
import logging
import sys


class Config:

    def __init__(self):
        self.file_path = 'data/1800_books/1820_1829.zip'
        self.decade = os.getenv('IMPORT_DECADE', '1820_1829_test_2')
        self.windows = [3, 5]
        self.output_folder = os.curdir

        self.alto_confidence = 0.3
        self.stop_words = {'a', 'an', 'the'}

        self.debug_limit_lines = 10_000
        # self.debug_limit_lines = -1

        self.system_workers = 1
        # self.system_workers = multiprocessing.cpu_count() - 1
        self.system_queue_size = 10
        # self.system_queue_size = 1000
        self.system_counter_dump_check_interval = 50_000
        # self.system_counter_dump_check_interval = 20_000
        self.system_counter_size_limit = 1_00
        # self.system_counter_size_limit = 100_000
        self.cache_max_size = 2_000_000
        # self.cache_max_size = 1_000_000 #100_000 = 2.2gb per process
        self.cache_init_size = 10
        # self.cache_init_size = 10_000

        self.mongo_host = os.getenv('MONGO_URI', 'localhost:27017')
        self.mongo_login = os.getenv('MONGO_INITDB_ROOT_USERNAME', 'root')
        self.mongo_pw = os.getenv('MONGO_INITDB_ROOT_PASSWORD', 'example')
        self.mongo_db = os.getenv('MONGO_DB_NAME', 'alto')
        self.mongo_collection = os.getenv('MONGO_COLL_NAME', '1820_test')

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
