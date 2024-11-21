import logging

from config import Config
from sqlite_client import SqliteClient


class Reporter:
    def __init__(self):
        self.config = Config()
        self.db = SqliteClient()

    def generate_report(self, worker_ids: list[int]):
        logging.info(f"Start report generation from workers: {worker_ids}")

        self.db.merge_into_result(worker_ids)