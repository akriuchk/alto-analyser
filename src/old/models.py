# import logging
# from collections import Counter
# from dataclasses import dataclass, asdict, field
#
# from typing import Optional
#
# from config import Config
# from counter_db import CounterDB
#
# config = Config()
#
# @dataclass
# class Stats:
#     window: int
#     counter: Counter
#     token_bag: list[str]
#
#     # @functools.cache to add cache here I need to make Stats class hasheable with impl of __hash__ method
#     def total_neighbors(self) -> int:
#         # logging.debug("Initiate total neighbors calculation and cache the value")
#         return self.counter.total()
#
#     def get_distance_prob(self, top_n) -> list:
#         result = []
#
#         total = self.total_neighbors()
#         for token, amount in self.counter.most_common(top_n):
#             result.append((token, amount / total))
#
#         for i in range(len(result), top_n):
#             result.append(('<none>', 0))
#
#         return result
#
#     def recalculate_distance_stats(self):
#         self.counter.update(self.token_bag)
#         self.token_bag.clear()
#
# @dataclass
# class NewToken:
#     word: str
#     word_bag: list[str]
#     stats: dict[int, CounterDB]
#     frequency: int = field(default=1)
#     version: int = field(default=1)
#     id: Optional[str] = field(default=None)
#
#     def get_stats(self, window: int) -> Counter:
#         result = self.stats.get(window)
#         if result is None:
#             result =  CounterDB()
#             self.stats[window] = result
#         return result
#
#
#     def get_stat_for_window(self, window: int, top_n: int) -> list:
#         """returns list of tuples"""
#         result = []
#         counter = self.get_stats(window)
#         total = counter.total()
#
#         for token, amount in counter.most_common(top_n):
#             result.append((token, amount / total))
#
#         for i in range(len(result), top_n):
#             result.append(('<none>', 0))
#
#         return result
#
#     def cleanup_counters(self):
#         updated_list = []
#         for window, counter in self.stats.items():
#             updated = counter.cleanup_tail(self.word,self.version, window)
#             if updated:
#                 updated_list.append((self.word, self.version))
#                 self.version += 1
#         if len(updated_list)>0:
#             logging.info(f"cleanup_counters of {updated_list}")
#
#
#     def __hash__(self):
#         return hash(self.word)
#
#
# @dataclass
# class Token:
#     token: str
#     stats: dict[int, Stats]  # window, stats
#     frequency: int
#     version: int = field(default=1)
#     id: Optional[str] = field(default=None)
#
#     def add(self, other):
#         self.frequency += other.frequency
#         for window, stats in other.stats.items():
#             self_counter = self.stats[window].counter
#             if self_counter is None:
#                 self.stats[window] = stats.counter
#             else:
#                 self_counter.update(stats.counter)
#
#
#     def recalculate_distance_bags(self):
#         for stat in self.stats.values():
#             stat.recalculate_distance_stats()
#
#     def get_stats(self, window: int) -> Stats:
#         result = self.stats.get(window)
#         if result is None:
#             return Stats(window, Counter(), [])
#         return result
#
#     def __hash__(self):
#         return hash(self.token)
#
#
# def dict_to_dataclass(data) -> NewToken:
#     stats: dict[str, dict] = data['stats']
#     return NewToken(
#         word=data['word'],
#         stats={int(window): CounterDB(stats_item) for window, stats_item in stats.items() if stats_item is not None},
#         word_bag=[],
#         frequency=data['frequency'],
#         id=data['_id'],
#         version=data['version']
#     )
#
# def dict_to_shallow_dataclass(data) -> NewToken:
#     return NewToken(
#         word=data['word'],
#         stats={window: CounterDB() for window in config.windows},
#         word_bag=[],
#         frequency=data['frequency'],
#         id=data['_id'],
#         version=data['version']
#     )
#
#
# def custom_dict_factory(data):
#     def convert(value):
#         if isinstance(value, Counter):
#             return {k[0]: k[1] for k in value.keys()}
#         elif isinstance(value, dict):
#             return {str(k): convert(v) for k, v in value.items()}
#         elif isinstance(value, list):
#             return [convert(item) for item in value]
#         return value
#
#     return {key: convert(value) for key, value in data}
#
#
# def dataclass_to_dict(instance):
#     return asdict(instance, dict_factory=custom_dict_factory)
