import logging
from pathlib import Path

from tqdm import tqdm

from config import Config
from dispatcher import Dispatcher
from processor.text_processor import tokenize, remove_stop_words
from processor.xml_processor import extract_text_lines
from processor.zip_processor import extract_xml_pages_from_zip


def sub_list_with_padding(line: list[str], word_idx: int, window: int, padding: str) -> list[str]:
    result: list[str] = []

    min_idx_before = word_idx - window
    max_idx_after = word_idx + window

    for i in range(min_idx_before, max_idx_after + 1):
        if 0 <= i < len(line):
            result.append(line[i])
        else:
            result.append(padding)

    return result


class FileReader:
    def __init__(self, file_path: str, windows: list[int], dispatcher: Dispatcher):
        self.file_path = file_path
        self.dispatcher = dispatcher
        self.windows = windows
        self.config = Config()

    def process_file(self):
        total_lines = self.config.total_lines_precalculated
        if total_lines == 0:
            logging.info("Start lines counting")

            with tqdm(desc="Counting lines", unit=" lines", mininterval=5, leave=True) as pbar:
                for page_xml in extract_xml_pages_from_zip(Path(self.file_path)):
                    for line_number, _ in enumerate(extract_text_lines(page_xml)):
                        if total_lines == self.config.debug_limit_lines:
                            logging.warning("limit reached(lines)")
                            break
            
                        total_lines += 1
                        pbar.update(1)
            
                    if total_lines == self.config.debug_limit_lines:
                        logging.warning("limit reached(pages)")
                        break

        logging.info("Start archive processing")

        with tqdm(total=total_lines, desc="Reading archive", unit=" lines", mininterval=5) as pbar:
            current_line = 0
            for page_xml in extract_xml_pages_from_zip(Path(self.file_path)):
                for line_number, line in enumerate(extract_text_lines(page_xml)):
                    # for line_number, line in enumerate(file):
                    if current_line == self.config.debug_limit_lines:
                        logging.warning("limit reached")
                        return

                    for token, sublist in self.__extract_tokens(line):
                        self.dispatcher.dispatch(token, sublist)
                        
                    pbar.update(1)
                    current_line += 1

    def __extract_tokens(self, line) -> tuple[str, list[str]]:
        words = remove_stop_words(tokenize(line))
        for word_idx, word in enumerate(words):
            padded_max_window_word_sublist = sub_list_with_padding(words, word_idx, max(self.windows), "<pad>")
            yield word, padded_max_window_word_sublist