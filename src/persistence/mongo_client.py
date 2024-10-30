import logging
from collections.abc import Iterable

from pymongo import MongoClient, ASCENDING, UpdateOne
from config.config import MONGODB
from models import dataclass_to_dict, Token

tokens_collection = 'tokens'

_db: MongoClient = None


def get_mongo_connection(collection: str):
    global _db
    if _db is None:
        try:
            logging.info("Connecting to MongoDB")
            uri = "mongodb://%s:%s@%s" % (
                MONGODB['login'], MONGODB['pass'], MONGODB['host'])

            client: MongoClient = MongoClient(uri)
            _db = client[MONGODB['database']]
            logging.debug("Connected to MongoDB successfully")

            __ensure_tokens_token_unique_idx()
        except Exception as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            raise

    return _db[collection]


def __drop_collection():
    if MONGODB['recreate']:
        get_mongo_connection(tokens_collection).drop_collection()


def __ensure_tokens_token_unique_idx():
    try:
        get_mongo_connection(tokens_collection).create_index(
            [("token", ASCENDING)],
            unique=True,
            name="tokens_token_unique_idx"
        )
        logging.info("Unique index on 'name' created in users collection")
    except Exception as e:
        logging.debug(f"Failed to create tokens_token_unique_idx: {e}")


def save_tokens(data):
    collection = get_mongo_connection(tokens_collection)
    try:
        if isinstance(data, Iterable):
            logging.debug(f"Start to convert data to json")
            documents = [dataclass_to_dict(item) for item in data]
            logging.debug(f"Converted")
            collection.insert_many(documents)
            logging.info(f"Inserted {len(data)} records into MongoDB")
        else:
            collection.insert_one(dataclass_to_dict(data))
            logging.info("Inserted 1 record into MongoDB")
    except Exception as e:
        logging.error(f"Failed to insert data into MongoDB: {e}")
        raise


def find_token(token: str):
    """could return None"""
    collection = get_mongo_connection(tokens_collection)
    return collection.find_one({'token': token})


def update_token_neighbors(data: Iterable[Token]):
    collection = get_mongo_connection(tokens_collection)

    updates = [
        UpdateOne({'token': token.token},{'$set': dataclass_to_dict(token)})
        for token in data
    ]

    result = collection.bulk_write(updates)
    logging.info(f"Updated: {result.modified_count}")


def find_all_tokens():
    collection = get_mongo_connection(tokens_collection)

    return collection.find()