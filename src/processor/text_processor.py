import logging

import regex as re

from config import Config

config = Config()

# nltk.download('stopwords')
# stop_words = set(nltk.corpus.stopwords.words('english'))

def tokenize(text: str) -> list[str]:
    return [t for t in re.findall(r'[\w-]*\p{L}[\w-]*', text)]


def remove_stop_words(tokens: list[str]) -> list[str]:
    return [t for t in tokens if t.lower() not in config.stop_words]