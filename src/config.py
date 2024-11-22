import multiprocessing
import os
import logging
import sys


class Config:
    def __init__(self):

        #path to source alto archive
        self.archive_path = 'data/in/1820_1829.zip'

        #path to output folder
        self.output_path = 'data/out'

        #name of decade, used to name output archives
        self.decade = '1820_1829'

        #list of windows for neighbour analysis
        self.windows = {3, 5}

        #alto OCR confidence filter threshold - such words are replaced with 'redacted' placeholder
        self.alto_confidence = 0.3

        #word removed from corpus
        self.stop_words = {'a', 'an', 'the'}

        #white list of words for analysis
        self.words_for_analysis = {'when', 'under', 'kind', 'first', 'my', 'good', 'you', 'die', 'some', 'great', 'your', 'love', 'heart', 'war'}

        #list of words excluded from final neighbour window analysis
        self.neighbour_filter = {'redacted', 'a', 'an', 'the'}

        #value is used for progress tracking, if it is >0 then automatic line counting is skipped
        #1820_1829 has 39208898 lines
        self.total_lines_precalculated=0

        #amount of lines to analyse before stop
        # -1 : full analysis
        # 260000 : minimum
        self.debug_limit_lines = -1

        #next parameters used for performance and memory usage tweaking

        #parameter to configure amount of workers = parallelization
        #by default = (amount of cpu cores in computer - 1)
        self.system_workers = multiprocessing.cpu_count() - 1

        #each worker has its own task queue
        self.system_queue_size = 1000

        #max size of in memory LRU cache
        self.cache_max_size = 2_500_000

        #after each N tokens dump cache overflow to DB
        self.system_counter_dump_check_interval = 100_000

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
