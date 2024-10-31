import logging
import os
from collections import Counter

from models import Stats
from persistence.mongo_client import open_change_stream, update_token_counters


def update_counter():
    logging.info(f'update_counter started on {os.getpid()}')
    for document in open_change_stream():
        try:
            token_document = document['fullDocument']
            logging.info(
                f"CDC caught for document: {document['ns']['db']}/{document['ns']['coll']}/{token_document['token']}/v{token_document['version']}/{token_document['_id']}")

            if document.get('updateDescription'):
                if len(document['updateDescription']['removedFields']):
                    logging.info("skip due to 'removedFields' in updateDescription")
                    continue

            distance_stats = []
            for stat in token_document['stats'].values():
                for distance, bag in stat['distance_bag'].items():
                    new_counter = Counter(bag)
                    distance_stat = stat['distance_stat'].get(distance)
                    if distance_stat is None:
                        distance_stat = new_counter
                    else:
                        distance_stat = Counter(distance_stat).update(new_counter)

                    distance_stats.append(Stats(stat['window'], {distance: distance_stat}, {}))

            update_token_counters(token_document['_id'], token_document['version'], distance_stats)

        except Exception:
            logging.error(f"Failed to process document: {document}")
            raise
