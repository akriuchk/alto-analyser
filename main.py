import time
from collections import Counter
from pathlib import Path
from typing import Set
from cProfile import Profile
from pstats import SortKey, Stats
from multiprocessing.pool import ThreadPool

from persistence import create_sqlite_database, add_token
from text_processor import tokenize, remove_stop_words, collect_window_stats, Token, merge_tokens_stats, \
    print_cache_stats
from xml_reader import extract_text_lines, avg_confidence
from zip_processor import extract_xml_pages_from_zip


def write_result(result_to_write: dict[str, Token], final_counter):
    start_write = time.time()
    print("start write to db")
    # token_sum: int = 0
    # for token_stat in result_to_write.values():
    #     token_sum+=token_stat.frequency


    total_token_sum: int = sum(final_counter.values())
    for token in result_to_write.values():
        add_token(decade, token, final_counter[token.token] / total_token_sum)
    print(f"[{len(result_to_write)}] done in {time.time() - start_write}s")

def call_stat_recalculation(token: Token) -> Token:
    token.recalculate_distance_bags()
    return token


if __name__ == '__main__':
    create_sqlite_database()

    c_page = 0
    c_lines = 0
    start = time.time()
    xml_text_size: int = 0
    text_size: int = 0
    counter: Counter = Counter()

    result: dict[str, Token] = {}

    decade = '1800_1809'

    # with (Profile() as profile):
    with ThreadPool(processes=16) as pool:
        for page_xml in extract_xml_pages_from_zip(Path(f'./data/1800_books/{decade}.zip')):
            xml_text_size += len(page_xml)
            # print(f"xml size: {xml_text_size}")
            try:
                tokens_to_recalculate_stats = []
                for line in extract_text_lines(page_xml):
                    if line != "":
                        # c_lines += 1
                        #log print(f"line[len={len(line)}]: {line}")
                        # text_size += len(line)
                        tokens = remove_stop_words(tokenize(line))
                        line_token_stats = collect_window_stats(result,tokens, [3, 5])
                        tokens_to_recalculate_stats.extend(line_token_stats)
                        counter.update(tokens)

                        # words |= set(tokenize(line))
                # print(counter)
                results = pool.map_async(call_stat_recalculation, tokens_to_recalculate_stats)
                # print("submitted")
                if c_page % 1000 == 0:
                    submitted_time = time.time()
                results.wait()
                if c_page % 1000 == 0:
                    print(f"{len(tokens_to_recalculate_stats)} recalc executed in {time.time() - submitted_time}s")
                tokens_to_recalculate_stats = []

            except ValueError as e:
                print("No sentences in xml: " + str(e))
            except StopIteration:
                print("No sentences in xml")
            c_page += 1
            if c_page % 10000 == 0:
                print(f"{time.time() - start}s: result size: {len(result)}, next page {c_page}")
                # print_cache_stats()
                # stats = Stats(profile).strip_dirs().sort_stats(SortKey.CUMULATIVE).print_stats()
                #
                # print("stats.calc_callees()")
                # # stats.calc_callees()
                # stats.print_callers("_count_elements")
                # exit(1)

            # print(f"avg time per page: {c_page} / {time.time() - start} = {c_page / (time.time() - start)}s")

            # write_result(result, counter)

            # if c_page >= 239:
            #     print(f"text size: {(text_size / xml_text_size) * 100}%; {text_size}")
            #     print(f"amount of words: {len(words)}")
            #     exit(1)
            # if c_lines >= 790:
                # print(f"avg_confidence={avg_confidence()}")
                # print(result)

                # write_result(result, counter)

                # print(result)
                # exit(1)

