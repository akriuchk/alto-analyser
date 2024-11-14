# import logging
# import random
# from collections.abc import Iterable
#
# from pymongo import MongoClient, ASCENDING, UpdateOne, WriteConcern
#
# from config import Config
# from old.models import dataclass_to_dict, Token, Stats, NewToken
#
# _db: MongoClient = None
#
# config = Config()
# tokens_collection = config.mongo_collection
#
# update_write_concern = WriteConcern(w=1)  # https://www.mongodb.com/docs/manual/reference/write-concern/
#
#
# def __get_mongo_connection(collection: str):
#     global _db
#     if _db is None:
#         try:
#             logging.info("Connecting to MongoDB")
#             uri = "mongodb://%s:%s@%s/?directConnection=true" % (
#                 config.mongo_login, config.mongo_pw, config.mongo_host)
#
#             client: MongoClient = MongoClient(uri)
#             _db = client[config.mongo_db]
#             logging.debug("Connected to MongoDB successfully")
#
#             __ensure_tokens_token_unique_idx()
#             # __drop_collection()
#         except Exception as e:
#             logging.error(f"Failed to connect to MongoDB: {e}")
#             raise
#
#     return _db[collection]
#
#
# # def __drop_collection():
# # if MONGODB['recreate']:
# #     get_mongo_connection(tokens_collection).drop()
#
#
# def __ensure_tokens_token_unique_idx():
#     try:
#         __get_mongo_connection(tokens_collection).create_index(
#             [("word", ASCENDING)],
#             unique=True,
#             name="tokens_word_unique_idx"
#         )
#         logging.info("Unique index on 'word' created in tokens collection")
#     except Exception as e:
#         logging.debug(f"Failed to create tokens_token_unique_idx: {e}")
#
#
# def save_tokens(data):
#     collection = __get_mongo_connection(tokens_collection)
#     try:
#         if isinstance(data, Iterable):
#             logging.debug(f"Start to convert data to json")
#             documents = [__to_document(item) for item in data]
#             logging.debug(f"Converted")
#             collection.insert_many(documents)
#             logging.debug(f"Inserted: {len(data)} documents")
#         else:
#             collection.insert_one(dataclass_to_dict(data))
#             logging.info("Inserted 1 record into MongoDB")
#     except Exception as e:
#         logging.error(f"Failed to insert data into MongoDB: {e}")
#         raise
#
#
# def __to_document(item) -> dict:
#     document = dataclass_to_dict(item)
#     document['rnd_idx'] = random.randint(0, 100)
#
#     return document
#
#
# def find_token(token: str):
#     """could return None"""
#     collection = __get_mongo_connection(tokens_collection)
#     return collection.find_one({'word': token}, projection=['word', 'frequency', 'version'])
#
#
# def find_token_by_id(id: str, version: int):
#     """could return None"""
#     collection = __get_mongo_connection(tokens_collection)
#     return collection.find_one({'_id': id, 'version': version})
#
#
# def find_all_tokens():
#     collection = __get_mongo_connection(tokens_collection)
#
#     return collection.find().batch_size(100_000)
#
#
# def count_all_tokens() -> int:
#     collection = __get_mongo_connection(tokens_collection)
#
#     return collection.count_documents({})
#
#
# def find_top_tokens(search_limit: int = 1000):
#     collection = __get_mongo_connection(tokens_collection)
#
#     return collection.find().sort("frequency", -1).limit(search_limit)
#
#
# def update_tokens_simple(data: Iterable[NewToken]):
#     collection = __get_mongo_connection(tokens_collection)
#
#     updates = [
#         UpdateOne(
#             {'token': token.word},
#             {
#                 "$set": {"frequency": token.frequency},
#                 "$inc": {"version": 1}
#             }
#         )
#         for token in data
#     ]
#
#     result = collection.with_options(write_concern=update_write_concern).bulk_write(updates)
#     logging.info(f"Updated: {result.modified_count} documents")
#
#
# def update_token_neighbors(data: Iterable[Token]):
#     collection = __get_mongo_connection(tokens_collection)
#
#     # "$push": {"stats.3.distance_stat.1": new_value}
#
#     def get_push_operations(token: Token) -> dict[str: dict]:
#         push_dict = {}
#         for stat in token.stats.values():
#             push_dict.update({f'stats.{stat.window}.token_bag': {"$each": stat.token_bag}})
#         return push_dict
#
#     updates = [
#         UpdateOne(
#             {'token': token.token},
#             {
#                 "$set": {"frequency": token.frequency},
#                 "$inc": {"version": 1},
#                 "$push": get_push_operations(token)
#             }
#
#         )
#         for token in data
#     ]
#
#     result = collection.with_options(write_concern=update_write_concern).bulk_write(updates)
#     logging.info(f"Updated: {result.modified_count} documents")
#
#
# def very_quick_update(id: str, version: int, stats: list[Stats]):
#     collection = __get_mongo_connection(tokens_collection)
#     set_stat_ops = {}
#     unset_bag_ops = {}
#
#     for s in stats:
#         # set_stat_ops[f'stats.{s.window}.counter'] = s.counter
#         set_stat_ops[f'stats.{s.window}.token_bag'] = []
#
#     result = collection.find_one_and_update(
#         {'_id': id, 'version': version},
#         {
#             "$inc": {"version": 1},
#             "$set": set_stat_ops,
#             "$unset": unset_bag_ops
#         }
#     )
#
#
# def update_token_counters(tokens: Iterable[NewToken]):
#     collection = __get_mongo_connection(tokens_collection)
#     if len(tokens) != len(set(tokens)):
#         logging.error("len(tokens) != len(set(tokens))")
#
#     def inc_counters_ops(t: NewToken) -> dict:
#         set_stat_ops = {}
#         for window, counter in t.stats.items():
#             for word, count in counter.items():
#                 set_stat_ops[f'stats.{window}.{word}'] = count
#
#         return set_stat_ops
#
#     update_batch = []
#     inc_counter = 0
#     inc_limit = 50_000
#
#     for token in tokens:
#         counter_increments = inc_counters_ops(token)
#
#         update_batch.append(UpdateOne(
#             {'word': token.word, 'version': token.version},
#             {
#                 "$set": {
#                     "frequency": token.frequency,
#                     "word_bag": []
#                 },
#                 "$inc": {
#                     "version": 1,
#                     **counter_increments
#                 }
#             }
#         ))
#
#         inc_counter += len(counter_increments)
#         if inc_counter >= inc_limit:
#             logging.info(f"Execute bulk_write: len(update_batch)={len(update_batch)}, inc_counter={inc_counter}")
#
#             for update in update_batch:
#                 update_search_query = update._filter
#
#                 stored_token = find_token(update_search_query['word'])
#
#                 if update_search_query['version'] != stored_token['version']:
#                     logging.info(f"missmatch: {update_search_query} != {stored_token}")
#
#                     for tkn in tokens:
#                         if update_search_query['word'] == tkn.word:
#                             logging.info(f"token: {tkn}")
#
#             result = collection.with_options(write_concern=update_write_concern).bulk_write(update_batch)
#             if result.matched_count != len(update_batch):
#                 logging.error("Some tokens were not found!")
#                 logging.info(f"updates={len(update_batch)}, result:{result.bulk_api_result}")
#                 # logging.info(f"update_batch: {update_batch}")
#
#                 for update in update_batch:
#                     update_search_query = update._filter
#
#                     stored_token = find_token(update_search_query['word'])
#
#                     if update_search_query['version'] == stored_token['version']:
#                         logging.info(f"missmatch: {update_search_query} == {stored_token}")
#
#                 # for e in result.bulk_api_result.values():
#             update_batch = []
#             inc_counter = 0
#     if len(update_batch) > 0:
#         if len(update_batch) > 1:
#             logging.info(f"Execute bulk_write of overflow: len(update_batch)={len(update_batch)}, inc_counter={inc_counter}")
#
#         for update in update_batch:
#             update_search_query = update._filter
#
#             stored_token = find_token(update_search_query['word'])
#
#             if update_search_query['version'] != stored_token['version']:
#                 logging.info(f"missmatch: {update_search_query} != {stored_token}")
#
#
#         result = collection.with_options(write_concern=update_write_concern).bulk_write(update_batch)
#         if result.matched_count != len(update_batch):
#             logging.error("Some tokens were not found!1")
#             logging.info(f"result: {result.bulk_api_result}")
#             # logging.info(f"update_batch: {update_batch}")
#
#             for update in update_batch:
#                 update_search_query = update._filter
#
#                 stored_token = find_token(update_search_query['word'])
#
#                 if update_search_query['version'] == stored_token['version']:
#                     logging.info(f"missmatch: {update_search_query} == {stored_token}")
#
#     # logging.info(f"Updated: {result.modified_count} documents")
#
#
# def increment_token_counters(word: str, version: int, window: int, counters: dict[str, int]):
#     collection = __get_mongo_connection(tokens_collection)
#
#     inc_stat_ops = {}
#     for w, count in counters.items():
#         inc_stat_ops[f'stats.{window}.{w}'] = count
#
#     updates = [
#         UpdateOne(
#             {'word': word, 'version': version},
#             {"$inc": {
#                 "version": 1,
#                 **inc_stat_ops
#             }}
#         )
#     ]
#
#     # logging.info(f"Execute bulk_write of token_counters for '{word}': len(counters)={len(counters)}, updates={len(updates)}")
#     result = collection.with_options(write_concern=update_write_concern).bulk_write(updates)
#     if result.matched_count != len(updates):
#         logging.error("Some tokens were not found!2")
#
#
# # def increment_token_counters(temp_token: NewToken):
# #     collection = __get_mongo_connection(tokens_collection)
# #
# #     def get_inc_ops(token: NewToken) -> dict:
# #         inc_stat_ops = {}
# #         for window, counter in token.stats.items():
# #             for word, count in counter.items():
# #                 inc_stat_ops[f'stats.{window}.{word}'] = count
# #         return inc_stat_ops
# #
# #     updates = [
# #         UpdateOne(
# #             {'word': temp_token.word, 'version': temp_token.version},
# #             {"$inc": {
# #                 "version": 1,
# #                 **get_inc_ops(temp_token)
# #             }}
# #         )
# #     ]
# #
# #     result = collection.with_options(write_concern=update_write_concern).bulk_write(updates)
# #     if result.matched_count != len(updates):
# #         logging.error("Some tokens were not found!")
