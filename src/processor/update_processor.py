import logging
import os

from persistence.mongo_client import open_change_stream, find_token


def update_counter():
    logging.info(f'update_counter started on {os.getpid()}')
    for document in open_change_stream():
        logging.info(f"CDC caught for document: {document}")
        find_token(document['fullDocument']['token'])
