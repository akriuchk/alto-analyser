import os
import logging


class Config:

    def __init__(self):
        self.file_path = 'data/1800_books/txt/1810_1819.txt'
        self.decade = os.getenv('IMPORT_DECADE', '1810_1819')
        self.windows = [3, 5]
        self.output_folder = os.curdir

        # self.debug_limit_lines = 20_000
        self.debug_limit_lines = -1

        # self.system_workers = 1
        self.system_workers = 6
        # self.system_queue_size = 10
        self.system_queue_size = 100  # Maximum number of items per queue

        # self.cache_max_size = 100
        self.cache_max_size = 90_000 #100_000 = 2.2gb per process
        # self.cache_init_size = 10
        self.cache_init_size = 10_000

        self.mongo_host = os.getenv('MONGO_URI', 'localhost:27017')
        self.mongo_login = os.getenv('MONGO_INITDB_ROOT_USERNAME', 'root')
        self.mongo_pw = os.getenv('MONGO_INITDB_ROOT_PASSWORD', 'example')
        self.mongo_db = os.getenv('DB_NAME', 'alto')

    def to_json(self):
        return self.__dict__


DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() == 'true'

# Logger configuration
logging.basicConfig(
    level=logging.DEBUG if DEBUG_MODE else logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)-5s [%(process)d][%(filename)s:%(lineno)d] %(message)s',
    datefmt='%m/%d %H:%M:%S'
)
logger = logging.getLogger('alto')
