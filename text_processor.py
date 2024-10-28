import functools
from collections import Counter, defaultdict
from typing import List, Set
from webbrowser import Error

import nltk
import regex as re
from dataclasses import dataclass

from cache import Cache

# nltk.download('stopwords')
# stop_words = set(nltk.corpus.stopwords.words('english'))
stop_words = {'a', 'an', 'the'}

c = Cache(cache_len=100000)
cache_hit: int = 0
cache_miss: int = 0

def tokenize(text: str) -> List[str]:
    return [t for t in re.findall(r'[\w-]*\p{L}[\w-]*', text)]


def remove_stop_words(tokens: List[str]) -> List[str]:
    return [t for t in tokens if t.lower() not in stop_words]


@dataclass
class Stats:
    window: int
    distance_stat: dict[int, Counter]
    distance_bag: dict[int, List[str]]

    # @functools.cache to add cache here I need to make Stats class hasheable with impl of __hash__ method
    def total_neighbors(self) -> int:
        print("Initiate total neighbors calculation and cache the value")
        return sum([sum(v.values()) for v in self.distance_stat.values()])

    def get_distance_prob(self) -> dict[int, float]:
        result = {window: 0.0 for window in range(1, self.window + 1)}
        for distance, counter in self.distance_stat.items():
            result[distance] = counter.most_common(1)[0][1] / sum(counter.values())

        return result

    def recalculate_distance_stats(self):
        for distance, token_bag in self.distance_bag.items():
            stat = self.distance_stat.get(distance)
            if stat is None:
                self.distance_stat[distance] = Counter(token_bag)
            else:
                stat.update(token_bag)
        self.distance_bag.clear()


@dataclass
class Token:
    token: str
    stats: dict[int, Stats]  # window, counter
    frequency: int

    def add(self, other):
        self.frequency += other.frequency
        for window, stats in other.stats.items():
            self_window_distance_stats = self.stats[window].distance_stat
            for distance, counter in stats.distance_stat.items():
                self_counter = self_window_distance_stats.get(distance)
                if self_counter is None:
                    self_window_distance_stats[distance] = counter
                else:
                    self_counter.update(counter)

    def recalculate_distance_bags(self):
        for stat in self.stats.values():
            stat.recalculate_distance_stats()


def collect_window_stats(result: dict[str, Token], tokens: List[str], windows: List[int]) -> List[Token]:
    #log print(f"Analyze {tokens} with windows: {windows}")
    updated_tokens: List[Token] = []
    data = {
        'man': {
            3: {
                # Соседние слова при запросе 3 соседних слова: такое то (% случаев от всех раз когда есть слово), такое то …, в архиве 1800, в архиве 1810…
                1: [{'mr': 10}, {'bla': 3}, {'john': 1}],
                2: {'john': 5},
                3: {'mayor': 3},
                'total_neighbors': 30
            },
            5: {
                1: {'mr': 20},
                2: {'john': 12},
                3: {'mayor': 8},
                4: {'thomas': 5},
                5: {'payed': 3},
                'total_neighbors': 60
            }
        },
        'paid': {
            3: {},
            5: {}
        }
    }

    for tokenIdx, token in enumerate(tokens):
        try:
            #log print(f"process token: {token}")
            result_token: Token = c.get(token)
            if result_token is None:
                global cache_miss
                cache_miss += 1

                result_token = result.get(token)
                if result_token is None:
                    result_token = Token(token, {window: Stats(window, {}, {}) for window in windows}, 0)
                    result[token] = result_token

                c[token] = result_token
            else:
                global cache_hit
                cache_hit += 1

            result_token.frequency += 1
            updated_tokens.append(result_token)
            token_range = range(0,  len(tokens))

            for distance in range(1, max(windows) + 1):
                tokens_to_update = []
                ngram_token_before_idx = tokenIdx - distance

                if ngram_token_before_idx in token_range:
                    neighbor_token_before = tokens[ngram_token_before_idx]
                    tokens_to_update.append(neighbor_token_before)

                ngram_token_after_idx = tokenIdx + distance
                if ngram_token_after_idx in token_range:
                    neighbor_token_after = tokens[ngram_token_after_idx]
                    tokens_to_update.append(neighbor_token_after)

                for window in windows:
                    if distance <= window:
                        __put_to_neighbor_bag(result_token, window, distance, tokens_to_update)

                # ngram_token_before_idx = tokenIdx - distance
                # if ngram_token_before_idx in range(0, len(tokens)):
                #     neighbor_token_before = tokens[ngram_token_before_idx]
                #     tokens_to_update.append(neighbor_token_before)
                #
                #     for window in windows:
                #         if distance <= window:
                #             __update_neighbor_stat(result_token, window, distance, neighbor_token_before)
                #
                # ngram_token_after_idx = tokenIdx + distance
                # if ngram_token_after_idx in range(0, len(tokens)):
                #     neighbor_token_after = tokens[ngram_token_after_idx]
                #     tokens_to_update.append(neighbor_token_after)
                #
                #     for window in windows:
                #         if distance <= window:
                #             __update_neighbor_stat(result_token, window, distance, neighbor_token_after)
        except AttributeError as e:
            print(f"Failed to process token {token}")
            raise e
    return updated_tokens


def __put_to_neighbor_bag(result_token, window:int , distance: int, neighbor_tokens: list[str]):
    window_stat: Stats = result_token.stats[window]
    distance_bag = window_stat.distance_bag.get(distance)
    if distance_bag is None:
        distance_bag = []
        window_stat.distance_bag[distance] = distance_bag
    distance_bag += neighbor_tokens


def __update_neighbor_stat(result_token, window:int , distance: int, neighbor_tokens: list[str]):
    window_stat: Stats = result_token.stats[window]
    distant_stat = window_stat.distance_stat.get(distance)
    if distant_stat is None:
        distant_stat = Counter()
        window_stat.distance_stat[distance] = distant_stat
    distant_stat.update(neighbor_tokens)


def merge_tokens_stats(original: dict[str, Token], adding: dict[str, Token]):
    for token, adding_token_stat in adding.items():
        original_token_stat = original.get(token)
        if original_token_stat is None:
            original[token] = adding_token_stat
        else:
            original_token_stat.add(adding_token_stat)


def print_cache_stats():
      print(f"cache stats: size: {len(c)} "
            f"hits: {cache_hit}({cache_hit / (cache_hit + cache_miss):.3f})"
            f",miss: {cache_miss}({cache_miss / (cache_hit + cache_miss):.3f})")