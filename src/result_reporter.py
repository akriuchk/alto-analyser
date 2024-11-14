# import logging
# from tqdm import tqdm
#
# import old.sqlite_client
# from config import Config
# from old.mongo_client import config
# from old.storage import Storage
# from old.sqlite_client import create_sqlite_database, write_token_batch
# from itertools import batched
#
#
# class Reporter:
#     def __init__(self):
#         self.config: Config()
#         self.file_path = config.file_path
#         self.windows = config.windows
#         self.store: Storage = Storage(cache=False)
#
#     def generate_report(self):
#         logging.info("Start report generation")
#         create_sqlite_database()
#         total = self.store.count_all()
#
#         if total == 0:
#             raise ValueError("Nothing found to report!")
#
#         progress_bar = tqdm(desc="Writing report", total=total, mininterval=5, unit=" tokens", leave=False)
#
#         batch_size = 100_000
#         for batch in batched(self.store.stream_all(), batch_size):
#             write_token_batch(batch, total)
#             progress_bar.update(batch_size)
#
#         progress_bar.close()
#         logging.info(f"Finished report generation file={old.sqlite_client.db_name}")
