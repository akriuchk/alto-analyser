from collections import Counter
from dataclasses import dataclass, asdict, make_dataclass, field

import logging
from typing import Optional


@dataclass
class Stats:
    window: int
    distance_stat: dict[int, Counter]
    distance_bag: dict[int, list[str]]

    # @functools.cache to add cache here I need to make Stats class hasheable with impl of __hash__ method
    def total_neighbors(self) -> int:
        logging.debug("Initiate total neighbors calculation and cache the value")
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
    id: Optional[str] = field(default=None)

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

    def get_stats(self, window: int) -> Stats:
        result = self.stats.get(window)
        if result is None:
            return Stats(window, {}, {})
        return result

    def __hash__(self):
        return hash(self.token)


def dict_to_dataclass(data) -> Token:
    stats: dict[str, dict] = data['stats']
    return Token(
        token=data['token'],
        stats={int(window): Stats(
            window=int(window),
            distance_stat={int(distance): Counter(counter)
                           for distance, counter
                           in stats_item['distance_stat'].items()},
            distance_bag={}
        ) for window, stats_item in stats.items()},
        frequency=data['frequency'],
        id=data['_id']
    )


def custom_dict_factory(data):
    def convert(value):
        if isinstance(value, Counter):
            return {k[0]: k[1] for k in value.keys()}
        elif isinstance(value, dict):
            return {str(k): convert(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [convert(item) for item in value]
        return value

    return {key: convert(value) for key, value in data}


def dataclass_to_dict(instance):
    return asdict(instance, dict_factory=custom_dict_factory)
