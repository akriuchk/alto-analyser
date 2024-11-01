import logging
import random
from collections.abc import Iterable

from pymongo import MongoClient, ASCENDING, UpdateOne, ReturnDocument

from config.config import MONGODB, IMPORT
from models import dataclass_to_dict, Token, Stats

tokens_collection = 'tokens'

_db: MongoClient = None


def get_mongo_connection(collection: str):
    global _db
    if _db is None:
        try:
            logging.info("Connecting to MongoDB")
            uri = "mongodb://%s:%s@%s/?directConnection=true" % (
                MONGODB['login'], MONGODB['pass'], MONGODB['host'])

            client: MongoClient = MongoClient(uri)
            _db = client[MONGODB['database']]
            logging.debug("Connected to MongoDB successfully")

            __ensure_tokens_token_unique_idx()
            # __drop_collection()
        except Exception as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            raise

    return _db[collection]


def __drop_collection():
    if MONGODB['recreate']:
        get_mongo_connection(tokens_collection).drop()


def __ensure_tokens_token_unique_idx():
    try:
        get_mongo_connection(tokens_collection).create_index(
            [("token", ASCENDING)],
            unique=True,
            name="tokens_token_unique_idx"
        )
        logging.info("Unique index on 'token' created in tokens collection")
    except Exception as e:
        logging.debug(f"Failed to create tokens_token_unique_idx: {e}")


def save_tokens(data):
    collection = get_mongo_connection(tokens_collection)
    try:
        if isinstance(data, Iterable):
            logging.debug(f"Start to convert data to json")
            documents = [__to_document(item) for item in data]
            logging.debug(f"Converted")
            collection.insert_many(documents)
            logging.info(f"Inserted: {len(data)} documents")
        else:
            collection.insert_one(dataclass_to_dict(data))
            logging.info("Inserted 1 record into MongoDB")
    except Exception as e:
        logging.error(f"Failed to insert data into MongoDB: {e}")
        raise

def __to_document(item) -> dict:
    document = dataclass_to_dict(item)
    document['rnd_idx'] = random.randint(0, 100)
    return document

def find_token(token: str):
    """could return None"""
    collection = get_mongo_connection(tokens_collection)
    return collection.find_one({'token': token})


def find_token_by_id(id: str, version: int):
    """could return None"""
    collection = get_mongo_connection(tokens_collection)
    return collection.find_one({'_id': id, 'version': version})


def find_all_tokens():
    collection = get_mongo_connection(tokens_collection)

    return collection.find()


def find_top_tokens(search_limit: int = 1000):
    collection = get_mongo_connection(tokens_collection)

    return collection.find().sort("frequency", -1).limit(search_limit)


def open_change_stream(rem: int):
    collection = get_mongo_connection(tokens_collection)
    pipeline = [
        # {"$match": {
        #     "$expr": {
        #         "$gt": [{"$bsonSize": "$$ROOT"}, 16*1024]  # Only documents larger than 4KB
        #         # "$gt": [{"$bsonSize": "$$ROOT"}, 4 * 1024 * 1024]  # Only documents larger than 4MB
        #     }
        # }},
        {"$match": {
            "operationType": "update",
            "$expr": {
                "$eq": [{ "$mod": ["$fullDocument.rnd_idx", IMPORT['workers']] }, rem],
            }
        }},
        {"$project": {"updateDescription": 0}}

        # {"$match": {
        #     "$or": [
        #         {"$expr": {
        #             "$and": [{
        #                         "$ne": [
        #                             {
        #                                 "$getField": {
        #                                     "field": "token_bag",
        #                                     "input": { "$getField": { "field": "5", "input": "$stats" } }
        #                                 }
        #                             },
        #                             None
        #                         ]
        #                     },
        #                     {
        #                         "$gt": [
        #                             {
        #                                 "$size": {
        #                                     "$getField": {
        #                                         "field": "token_bag",
        #                                         "input": { "$getField": { "field": "5", "input": "$stats" } }
        #                                     }
        #                                 }
        #                             },
        #                             1000
        #                         ]
        #                     }
        #                 # "$gt": [{"$bsonSize": "$$ROOT"}, 4*1024]  # Only documents larger than 4KB
        #                 # "$gt": [{"$bsonSize": "$$ROOT"}, 1 * 1024 * 1024]  # Only documents larger than 4MB
        #             ]
        #         }},
        #         {"$expr": {
        #             "$and": [{
        #                         "$ne": [
        #                             {
        #                                 "$getField": {
        #                                     "field": "token_bag",
        #                                     "input": { "$getField": { "field": "3", "input": "$stats" } }
        #                                 }
        #                             },
        #                             None
        #                         ]
        #                     },
        #                     {
        #                         "$gt": [
        #                             {
        #                                 "$size": {
        #                                     "$getField": {
        #                                         "field": "token_bag",
        #                                         "input": { "$getField": { "field": "3", "input": "$stats" } }
        #                                     }
        #                                 }
        #                             },
        #                             1000
        #                         ]
        #                     }
        #                 # "$gt": [{"$bsonSize": "$$ROOT"}, 4*1024]  # Only documents larger than 4KB
        #                 # "$gt": [{"$bsonSize": "$$ROOT"}, 1 * 1024 * 1024]  # Only documents larger than 4MB
        #             ]
        #         }}
        #     ]
        # }}
    ]
    return collection.watch(pipeline=pipeline, full_document="updateLookup")


def update_token_neighbors(data: Iterable[Token]):
    collection = get_mongo_connection(tokens_collection)

    # "$push": {"stats.3.distance_stat.1": new_value}

    def get_push_operations(token: Token) -> dict[str: dict]:
        push_dict = {}
        for stat in token.stats.values():
            push_dict.update({f'stats.{stat.window}.token_bag': {"$each": stat.token_bag}})
        return push_dict

    updates = [
        UpdateOne(
            {'token': token.token},
            {
                "$set": {"frequency": token.frequency},
                "$inc": {"version": 1},
                "$push": get_push_operations(token)
            }
        )
        for token in data
    ]

    result = collection.bulk_write(updates)
    logging.info(f"Updated: {result.modified_count} documents")


def very_quick_update(id: str, version: int, stats: list[Stats]):
    collection = get_mongo_connection(tokens_collection)
    set_stat_ops = {}
    unset_bag_ops = {}

    for s in stats:
        # set_stat_ops[f'stats.{s.window}.counter'] = s.counter
        set_stat_ops[f'stats.{s.window}.token_bag'] = []

    result = collection.find_one_and_update(
        {'_id': id, 'version': version},
        {
            "$inc": {"version": 1},
            "$set": set_stat_ops,
            "$unset": unset_bag_ops
        }
    )

def update_token_counters(id: str, version: int, stats: list[Stats]):
    collection = get_mongo_connection(tokens_collection)

    set_stat_ops = {}
    unset_bag_ops = {}
    for s in stats:
        set_stat_ops[f'stats.{s.window}.counter'] = s.counter
        set_stat_ops[f'stats.{s.window}.token_bag'] = []

    result = collection.find_one_and_update(
        {'_id': id, 'version': version},
        {
            "$inc": {"version": 1},
            "$set": set_stat_ops,
            "$unset": unset_bag_ops
        }, return_document=ReturnDocument.AFTER
    )

    if result:
        logging.debug(f"Document updated: {result['token']}, v{version} -> v{result['version']}")
    else:
        logging.debug(f"Document {id}/v{version} was not updated")

    return result
