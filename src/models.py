from dataclasses import dataclass


@dataclass
class TokenCounter:
    __slots__ = ("word", "window", "neighbor", "neighbor_frequency")
    word: str
    window: int
    neighbor: str
    neighbor_frequency: int
    # key: str  # word.window.neighbor
