import logging
import sqlite3
from collections import defaultdict
from itertools import batched

from config import Config
from models import TokenCounter

token_stats_table_columns = ["id",
                             "token",
                             "decade",
                             "frequency",
                             "probability",
                             "w_31", "p_31", "w_32", "p_32", "w_33", "p_33",
                             "w_51", "p_51", "w_52", "p_52", "w_53", "p_53", "w_54", "p_54", "w_55", "p_55"]

class SqliteClient:
    def __init__(self, worker_id: int):
        self.config = Config()
        self.db_name=f'{self.config.mongo_db}/{self.config.mongo_collection}.{worker_id}.sqlite'


    def get_connection(self):
        return sqlite3.connect(self.db_name, isolation_level=None)


    def create_sqlite_database(self):
        """ create a database connection to an SQLite database """
        with self.get_connection() as conn:
            conn.execute('PRAGMA journal_mode = OFF;')
            conn.execute('PRAGMA synchronous = 0;')
            conn.execute('PRAGMA cache_size = 200000;')  # give it a 0.1GB
            conn.execute('PRAGMA locking_mode = EXCLUSIVE;')
            conn.execute('PRAGMA temp_store = MEMORY;')

            self.create_tables(conn)

            conn.commit()


    def create_tables(self, conn):
        logging.info("create tables")
        #lang=SQL
        sql_statements = [
            """CREATE TABLE IF NOT EXISTS report (
                    id INTEGER KEY,
                    word TEXT NOT NULL,
                    decade TEXT,
                    frequency INT NOT NULL,
                    probability REAL,
                    w_31 TEXT,
                    p_31 REAL,
                    w_32 TEXT,
                    p_32 REAL,
                    w_33 TEXT,
                    p_33 REAL,
                    w_51 TEXT,
                    p_51 REAL,
                    w_52 TEXT,
                    p_52 REAL,
                    w_53 TEXT,
                    p_53 REAL,
                    w_54 TEXT,
                    p_54 REAL,
                    w_55 TEXT,
                    p_55 REAL,
                    PRIMARY KEY (word)
            );""",
            """CREATE TABLE IF NOT EXISTS decade (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
            );""",
            """CREATE TABLE IF NOT EXISTS token (
                word TEXT PRIMARY KEY,
                value JSON
            )"""
        ]

        # create a database connection
        for statement in sql_statements:
            conn.execute(statement)


    # "token",
    # "decade",
    # "frequency",
    # "probability",
    # "w_31", "p_31", "w_32", "p_32", "w_33", "p_33",
    # "w_51", "p_51", "w_52", "p_52", "w_53", "p_53", "w_54", "p_54", "w_55", "p_55"

    # def add_token(self, decade: str, token: NewToken, probability: float):
    #     try:
    #         with get_connection() as conn:
    #             insert_columns = token_stats_table_columns[1:]
    #             vals = "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
    #
    #             sql = f"""INSERT INTO {token_stats_table}({",".join(insert_columns)})
    #                       VALUES({vals})"""
    #             cur = conn.cursor()
    #
    #             top_3 = token.get_stat_for_window(3, 3)
    #             top_5 = token.get_stat_for_window(5, 5)
    #
    #             data = (token.word, decade, token.frequency, probability,
    #                     top_3[0][0],
    #                     top_3[0][1],
    #                     top_3[1][0],
    #                     top_3[1][1],
    #                     top_3[2][0],
    #                     top_3[2][1],
    #
    #                     top_5[0][0],
    #                     top_5[0][1],
    #                     top_5[1][0],
    #                     top_5[1][1],
    #                     top_5[2][0],
    #                     top_5[2][1],
    #                     top_5[3][0],
    #                     top_5[3][1],
    #                     top_5[4][0],
    #                     top_5[4][1],
    #                     )
    #
    #             cur.execute(sql, data)
    #             conn.commit()
    #         return cur.lastrowid
    #     except sqlite3.Error as e:
    #         print(f"ERROR: Failed to execute query sql:{sql}\ndata:{data}\ne: {e}")
    #
    #
    # def write_token_batch(self, tokens: Iterable, total_tokens: int):
    #     insert_columns = token_stats_table_columns[1:]
    #     vals = "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
    #
    #     sql = f"""INSERT INTO {token_stats_table}({",".join(insert_columns)}) VALUES({vals})"""
    #
    #     # con.executemany('INSERT INTO user VALUES (NULL,?,?,?)', current_batch)
    #     current_batch = []
    #
    #     for token_document in tokens:
    #         token = dict_to_dataclass(token_document)
    #         if token.word == 'redacted':
    #             continue
    #
    #         probability = token.frequency / total_tokens
    #         top_3 = token.get_stat_for_window(3, 3)
    #         top_5 = token.get_stat_for_window(5, 5)
    #
    #         data = (token.word, config.decade, token.frequency, probability,
    #                 top_3[0][0],
    #                 top_3[0][1],
    #                 top_3[1][0],
    #                 top_3[1][1],
    #                 top_3[2][0],
    #                 top_3[2][1],
    #
    #                 top_5[0][0],
    #                 top_5[0][1],
    #                 top_5[1][0],
    #                 top_5[1][1],
    #                 top_5[2][0],
    #                 top_5[2][1],
    #                 top_5[3][0],
    #                 top_5[3][1],
    #                 top_5[4][0],
    #                 top_5[4][1],
    #                 )
    #         current_batch.append(data)
    #
    #     try:
    #         with get_connection() as conn:
    #             conn.execute('BEGIN')
    #             conn.executemany(sql, current_batch)
    #             conn.commit()
    #
    #     except sqlite3.Error as e:
    #         print(f"ERROR: Failed to execute query sql:{sql}\ndata:{data}\ne: {e}")
    def append_words(self, word_counter: dict[str, int]):
        sql = f"""INSERT INTO report(word, frequency) VALUES(?,?)
                ON CONFLICT(word) DO UPDATE SET frequency=frequency+excluded.frequency
                """

        for batch in batched(word_counter.items(), 50_000):
            current_batch = [(word, frequency) for word, frequency in batch]
            try:
                with self.get_connection() as conn:
                    conn.execute('BEGIN')
                    conn.executemany(sql, current_batch)
                    conn.commit()

            except sqlite3.Error as e:
                print(f"ERROR: Failed to execute query sql:{sql}\ndata:{current_batch}\ne: {e}")

    def append_neighbor_counters(self, token_neighbor_counters: list[TokenCounter]):
        # sql_temp = """UPDATE token
        #                 SET
        #                     value = json_insert(
        #                     value,
        #                     '$.5.aboba[#]', 2,
        #                     '$.3.mcdac[#]', 3
        #                   )
        #                 WHERE
        #                   word = 'hello';
        #                 """

        updates: dict[str:list] = defaultdict(list)
        for counter in token_neighbor_counters:
            updates[counter.word].append(f'$.{counter.window}.{counter.neighbor}[#]')
            updates[counter.word].append(f'{counter.neighbor_frequency}')

        def log_sql_callback(statement):
            print("Executing SQL statement:", statement)

        with self.get_connection() as conn:
            try:
                conn.execute('BEGIN')
                # conn.set_trace_callback(log_sql_callback)
                conn.executemany("INSERT OR IGNORE INTO token(word, value) VALUES(?, ?)", [(w,'{}')  for w in updates.keys()])
                for word, update_tuple in updates.items():
                    for batch in batched(update_tuple, 60):
                        sql = f'UPDATE token SET value = jsonb_insert(value {",?" * len(batch)}) WHERE word = ?;'
                        u = list(batch);u.append(word)

                        conn.executemany(sql, [u])
                conn.commit()
            except sqlite3.Error as e:
                print(f"ERROR: Failed to execute query sql:{sql}\ndata:{update_tuple}\ne: {e}")
                raise e

