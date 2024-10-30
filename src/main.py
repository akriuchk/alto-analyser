import logging
import os
import time
from collections import Counter

import processor.text_processor
from config import config
from persistence.IStorage import IStorage
from persistence.sqlite_client import create_sqlite_database, add_token
from persistence.storage import Storage
from processor.text_processor import tokenize, collect_window_stats
from models import Token

def write_result(final_counter):
    result_to_write: dict[str, Token] = storage.find_all()
    start_write = time.time()
    logging.info("start write to db")
    # token_sum: int = 0
    # for token_stat in result_to_write.values():
    #     token_sum+=token_stat.frequency

    total_token_sum: int = sum(final_counter.values())

    try:
        for i, token in enumerate(result_to_write.values()):
            if i % 5000 == 0:
                logging.info(f"{i / len(result_to_write):.4f}")
            add_token(decade, token, final_counter[token.token] / total_token_sum)
        logging.info(f"[{len(result_to_write)}] done in {time.time() - start_write}s")
    except Exception as e :
        logging.error(f"failed to write a file, retry, {e}")

        os.rename("data/1800_books/output_data/alto_30_confidence_1800.db"
                  , "data/1800_books/output_data/alto_30_confidence_1800.0.db")

        for i, token in enumerate(result_to_write.values()):
            if i % 5000 == 0:
                logging.info(f"{i / len(result_to_write):.4f}")
            add_token(decade, token, final_counter[token.token] / total_token_sum)
        logging.info(f"[{len(result_to_write)}] done in {time.time() - start_write}s")


def call_stat_recalculation(token: Token) -> Token:
    token.recalculate_distance_bags()
    return token


if __name__ == '__main__':
    decade = config.IMPORT["decade"]

    create_sqlite_database()

    # cap = 5000

    c_page = 0
    c_lines = 0
    start = time.time()
    xml_text_size: int = 0
    text_size: int = 0
    counter: Counter = Counter()


    filepath = f'data/1800_books/txt'

    # os.makedirs(filepath, exist_ok=True)
    # with open(f"{filepath}/{decade}.txt", "w") as file:
    #     for page_xml in extract_xml_pages_from_zip(Path(f'./data/1800_books/{decade}.zip')):
    #         for line in extract_text_lines(page_xml):
    #             if line != "":
    #                 c_lines += 1
    #                 #log print(f"line[len={len(line)}]: {line}")
    #                 # text_size += len(line)
    #                 tokens = remove_stop_words(tokenize(line))
    #                 if len(tokens) > 0:
    #                     file.write(" ".join(tokens))
    #                     file.write("\n")
    #         c_page += 1
    #         if c_page % 10000 == 0:
    #             log.info(f"{time.time() - start}s: result size: {len(result)}, next page {c_page}")
    #         # if c_lines >= cap:
    #         #     print("cap reached")
    #         #     break

    storage:IStorage = Storage()
    processor.text_processor.storage = storage

    with open(f"{filepath}/{decade}.txt", "r") as file:
        updated_tokens: dict[str, Token] = {}
        for line_number, line in enumerate(file, start=1):
            tokens = tokenize(line)
            line_token_stats = collect_window_stats(updated_tokens, tokens, [3, 5])
            counter.update(tokens)

            if line_number % 5000 == 0:
                result_size = len(updated_tokens)
                logging.info(f"{time.time() - start:.2f}s: result size: {result_size}, next line {line_number},{line_number*100/17657703:.2f}%, {result_size / (time.time() - start):.1f} res/s")
                # wait for completion
                # will be done by workers
                for token in updated_tokens.values():
                    token.recalculate_distance_bags()

                storage.save_many(set(updated_tokens.values()))
                updated_tokens.clear()

            # if line_number >= cap:
            #     print("cap reached")
            #     break

            # if line_number % 1000000 == 0:
            #     write_result(result, counter)
            #
            #     print("done")
            #     exit(1)
    for token in updated_tokens.values():
        token.recalculate_distance_bags()
    updated_tokens.clear()

    write_result(counter)
    logging.info("done")