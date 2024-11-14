import logging
from collections import Counter

import regex as re

from models import Stats, Token
from persistence.IStorage import IStorage

# nltk.download('stopwords')
# stop_words = set(nltk.corpus.stopwords.words('english'))
stop_words = {'a', 'an', 'the'}

storage: IStorage

def tokenize(text: str) -> list[str]:
    return [t for t in re.findall(r'[\w-]*\p{L}[\w-]*', text)]


def remove_stop_words(tokens: list[str]) -> list[str]:
    return [t for t in tokens if t.lower() not in stop_words]


def collect_window_stats(result: dict[str, Token], tokens: list[str], windows: list[int]) -> list[Token]:
    logging.debug(f"Analyze {tokens} with windows: {windows}")
    updated_tokens: list[Token] = []
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
            # logging.debug(f"process token: {token}")

            result_token: Token = result.get(token)
            if result_token is None:
                result_token = storage.find_shallow(token)
                result[token] = result_token
            if result_token is None:
                result_token = Token(token, {window: Stats(window, Counter(), []) for window in windows}, 0)
                result[token] = result_token

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
                        __put_to_neighbor_bag(result_token, window, tokens_to_update)

        except AttributeError as e:
            logging.error(f"Failed to process token {token}")
            raise e
    return updated_tokens


def __put_to_neighbor_bag(result_token, window:int, neighbor_tokens: list[str]):
    if result_token.token == 'redacted' or len(neighbor_tokens) == 0:
        return

    window_stat: Stats = result_token.get_stats(window)
    window_stat.token_bag += neighbor_tokens


def __update_neighbor_stat(result_token, window:int , neighbor_tokens: list[str]):
    window_stat: Stats = result_token.stats[window]
    window_stat.counter.update(neighbor_tokens)


def merge_tokens_stats(original: dict[str, Token], adding: dict[str, Token]):
    for token, adding_token_stat in adding.items():
        original_token_stat = original.get(token)
        if original_token_stat is None:
            original[token] = adding_token_stat
        else:
            original_token_stat.add(adding_token_stat)
