import os
import logging

MONGODB = {
    'host': os.getenv('MONGO_URI', 'localhost:27017'),
    'login': os.getenv('MONGO_INITDB_ROOT_USERNAME', 'root'),
    'pass': os.getenv('MONGO_INITDB_ROOT_PASSWORD', 'example'),
    'database': os.getenv('DB_NAME', 'alto'),
    'recreate': os.getenv('DB_RECREATE', 'false'),
}

IMPORT = {
    'decade': os.getenv('IMPORT_DECADE', '1810_1819'),
    'output_folder': os.curdir,
    'workers': int(os.getenv('IMPORT_WORKERS', 4)),

}

DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() == 'true'

# Logger configuration
logging.basicConfig(
    level=logging.DEBUG if DEBUG_MODE else logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)-5s [%(process)d][%(filename)s:%(lineno)d] %(message)s',
    datefmt='%m/%d %H:%M:%S'
)
logger = logging.getLogger('alto')
