from dataclasses import dataclass


@dataclass
class TokenCounter:
    __slots__ = ("word", "window", "neighbour", "neighbour_frequency")
    word: str
    window: int
    neighbour: str
    neighbour_frequency: int
    # key: str  # word.window.neighbour
