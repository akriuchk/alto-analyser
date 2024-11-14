import logging

from models import NewToken


def analyze_word(token: NewToken):
    # logging.info(f"Analyzing word: {token}")
    token.frequency+=1

    if token.word == 'redacted':
        return

    windows = token.stats.keys()

    try:
        for distance in range(1, max(windows)):

            middle_idx = max(windows)
            ngram_token_before_idx = middle_idx - distance
            ngram_token_after_idx = middle_idx + distance

            word_before = token.word_bag[ngram_token_before_idx]
            word_after = token.word_bag[ngram_token_after_idx]

            for window, counter in token.stats.items():
                if distance <= window:
                    if word_before != '<pad>':
                        counter.update([word_before]) #todo refactor to do batch update(push work to CPython)
                    if word_after != '<pad>':
                        counter.update([word_after])
            

        token.word_bag.clear()
    except Exception as e:
        logging.error(f"Failed to process token: {token.word} with error {e}", exc_info=e)