import logging
from collections import Counter

from tqdm import tqdm

import config
from dispatcher import Dispatcher
from models import NewToken


class FileReader:
    def __init__(self, file_path: str, windows: list[int], dispatcher: Dispatcher):
        self.file_path = file_path
        self.dispatcher = dispatcher
        self.windows = windows

    def process_file(self):
        with open(self.file_path, 'r') as file:
            total_lines = sum(1 for _ in file)
            file.seek(0)  # Reset file pointer to the beginning


            with tqdm(total=total_lines, desc="Reading file", unit="line", mininterval=5) as pbar:
                for line_number, line in enumerate(file):
                    if line_number == config.Config().debug_limit_lines:
                        logging.warning("limit reached")
                        return

                    for token in self.__extract_tokens(line):
                        self.dispatcher.dispatch(token)
                    pbar.update(1)

    def __extract_tokens(self, line) -> list[NewToken]:
        tokens: list[NewToken] = []
        words = line.strip().split()
        for word_idx, word in enumerate(words):
            padded_max_window_word_sublist = self.__sub_list_with_padding(words, word_idx, max(self.windows), "<pad>")
            token: NewToken = NewToken(word=word, word_bag=padded_max_window_word_sublist, stats={window: Counter() for window in self.windows})
            tokens.append(token)
        return tokens

    def __sub_list_with_padding(self, line: list[str], word_idx: int, window: int, padding: str) -> list[str]:
        result: list[str] = []

        min_idx_before = word_idx - window
        max_idx_after = word_idx + window

        for i in range(min_idx_before, max_idx_after):
            if 0 <= i < len(line):
                result.append(line[i])
            else:
                result.append(padding)

        return result