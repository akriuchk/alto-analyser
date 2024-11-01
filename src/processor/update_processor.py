import logging
import os
from collections import Counter

from models import Stats
from persistence.mongo_client import open_change_stream, update_token_counters, very_quick_update

stats = {
    'consumed_total': 0,
    'filtered_removedFields': 0,
    'filtered_token_bag': 0,
    'processed': 0,
    'missed': 0
}

def process_change_stream(lock, rem):
    try:
        update_counter(lock, rem)
    except Exception as e:
        logging.error(f"Failed to process change stream {e} %s", e)
        lock.acquire()


def update_counter(lock, rem: int):
    logging.info(f'update_counter process worker[rem={rem}] started on pid[{os.getpid()}]')
    for document in open_change_stream(rem):
        stats['consumed_total'] +=1
        try:
            token_document = document['fullDocument']
            logging.debug(
                f"CDC caught for document: {document['ns']['db']}/{document['ns']['coll']}/{token_document['token']}/v{token_document['version']}/{token_document['_id']}")

            if document.get('updateDescription'):
                if len(document['updateDescription']['removedFields']):
                    logging.debug("skipped by 'removedFields' filter")
                    stats['filtered_removedFields'] +=1
                    continue

            is_any_bag_big = any([stats_values for stats_values in token_document['stats'].values() if stats_values.get('token_bag') is not None and len(stats_values['token_bag']) < 1000])
            if is_any_bag_big:
                logging.debug(f"skipped by 'token_bag' filter")
                stats['filtered_token_bag'] +=1
                continue

            is_any_bag_huge = any([stats_values for stats_values in token_document['stats'].values() if stats_values.get('token_bag') is not None and len(stats_values['token_bag']) > 100000])
            if is_any_bag_huge:
                lock.acquire()
                logging.info(f"Caught CDC for huge document, pause processing {token_document['token']}/v{token_document['version']}/{token_document['_id']}")

            try:
                distance_stats = []
                for stat in token_document['stats'].values():
                    token_bag = stat.get('token_bag')
                    if token_bag is None:
                        continue
                    new_counter = Counter(token_bag)
                    counter = stat.get('counter')
                    if counter is None:
                        counter = new_counter
                    else:
                        counter = Counter(counter)
                        counter.update(new_counter)

                    distance_stats.append(Stats(stat['window'], counter, []))

                result = update_token_counters(token_document['_id'], token_document['version'], distance_stats)
                if result:
                    stats['processed'] += 1
                else:
                    stats['missed'] += 1

                __log()
            finally:
                if is_any_bag_huge:
                    lock.release()
                    logging.info("continue")
        except Exception as exc:
            logging.error(f"Failed to process document: {document} with error {exc}")
            raise

def __log():
    if stats['consumed_total'] % 1000 == 0:
        logging.info(f"CDC stats: {stats}")