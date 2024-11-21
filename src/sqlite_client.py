import logging
import sqlite3
import time
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


def log(stmt):
    # logging.info(stmt)
    pass

class SqliteClient:
    def __init__(self, worker_id: int = None):
        self.config = Config()
        self.db_name = self.get_db_path(worker_id)


    def get_connection(self, worker_id: int = None):
        if worker_id is not None:
            return sqlite3.connect(self.get_db_path(worker_id), isolation_level=None, timeout=15)
        else:
            return sqlite3.connect(self.db_name, isolation_level=None,  timeout=15)


    def get_db_path(self, worker_id: int = None):
        if worker_id is not None:
            return f'{self.config.output_path}/{self.config.decade}.{worker_id}.sqlite'
        else:
            return f'{self.config.output_path}/{self.config.decade}.sqlite'


    def merge_into_result(self, worker_ids: list[int]):
        with self.get_connection() as result_db_conn:
            self.create_tables(result_db_conn)

            for worker_id in worker_ids:
                logging.info(f'merge db {worker_id}')

                result_db_conn.execute(f'ATTACH DATABASE "{self.get_db_path(worker_id)}" AS chunk_db')
                result_db_conn.execute('insert into tokens select * from chunk_db.tokens;')
                result_db_conn.execute(f'DETACH DATABASE chunk_db')


    def create_sqlite_database(self, conn):
        if conn is None:
            conn = self.get_connection()

        """ create a database connection to an SQLite database """
        conn.execute('PRAGMA journal_mode = OFF;')
        conn.execute('PRAGMA synchronous = 0;')
        conn.execute('PRAGMA cache_size = 200000;')  # give it a 0.1GB
        conn.execute('PRAGMA locking_mode = EXCLUSIVE;')
        conn.execute('PRAGMA temp_store = MEMORY;')

        self.create_tables(conn)
        conn.commit()


    def create_tables(self, conn):
        logging.info(f'create tables in {self.db_name}')
        #lang=SQL

        word_columns = []
        for window in self.config.windows:
            for idx in range(1, window+1):
                word_columns.append(f"w_{window}{idx} TEXT")
                word_columns.append(f"p_{window}{idx} REAL")

        sql_statements = [
            f"""CREATE TABLE IF NOT EXISTS tokens (
                    word TEXT NOT NULL,
                    frequency INT NOT NULL,
                    probability REAL,
                    {",".join(word_columns)},
                    PRIMARY KEY (word)
            );""",
            """CREATE TABLE IF NOT EXISTS token_neighbors(
                word TEXT NOT NULL,
                window INTEGER NOT NULL,
                neighbor TEXT NOT NULL,
                frequency INT NOT NULL DEFAULT 0,
                PRIMARY KEY (word, window, neighbor)
            )"""
        ]

        # create a database connection
        for statement in sql_statements:
            conn.execute(statement)

    def append_words(self, word_counter: dict[str, int]):
        sql = f"""INSERT INTO tokens(word, frequency) VALUES(?,?)
                ON CONFLICT(word) DO UPDATE SET frequency=frequency+excluded.frequency
                """

        for batch in batched(word_counter.items(), 50_000):
            current_batch = [(word, frequency) for word, frequency in batch]
            try:
                with self.get_connection() as conn:
                    conn.execute('BEGIN')
                    conn.set_trace_callback(log)
                    conn.executemany(sql, current_batch)
                    conn.commit()

            except sqlite3.Error as e:
                logging.error(f'ERROR: Failed to execute query sql:{sql}\ndata:{current_batch}',e)

    def append_neighbor_counters(self, token_neighbor_counters: list[TokenCounter]):
        sql = f"""INSERT INTO token_neighbors(word,window,neighbor,frequency) VALUES(?,?,?,?)
                ON CONFLICT(word,window,neighbor) DO UPDATE SET frequency=frequency+excluded.frequency
                """

        for batch in batched(token_neighbor_counters, 250_000):
            current_batch = []
            for counter in batch:
                current_batch.append((counter.word, counter.window, counter.neighbor, counter.neighbor_frequency))

            try:
                with self.get_connection() as conn:
                    conn.execute('BEGIN')
                    conn.executemany(sql, current_batch)
                    conn.commit()

            except sqlite3.Error as e:
                logging.error("db", self.db_name)
                logging.error(f'ERROR: Failed to execute query sql:{sql}\ndata:{str(current_batch)}',e)

    def fetch_analysed_tokens(self) -> list[str]:
        sql = "select distinct(word) from token_neighbors"

        try:
            with self.get_connection() as conn:
                return [row[0] for row in conn.execute(sql).fetchall()]

        except sqlite3.Error as e:
            logging.error(f'ERROR: Failed to execute query sql:{sql}', e)

    def get_token_window_neighbor_sum(self, token) -> dict[int, int]:
        # returns {3: 2010, 5: 2564}
        sql = f"""select "window", sum(frequency) 
                 from token_neighbors tn 
                 where word = '{token}'
                 group by word, "window";"""

        try:
            with self.get_connection() as conn:
                return {row[0]: row[1] for row in conn.execute(sql).fetchall()}

        except sqlite3.Error as e:
            logging.error(f'ERROR: Failed to execute query sql:{sql}', e)

    def get_top_n_neighbors(self, token, window) -> dict[str, int]:
        # returns {and: 77, of: 75, to: 68}
        exclusions = ",".join([f'\"{w}\"' for w in self.config.neighbour_filter])
        sql = f"""select neighbor, frequency from token_neighbors tn 
                where word = "{token}" and "window" = {window} and neighbor not in ({exclusions})
                order by frequency desc 
                limit {window}"""


        try:
            with self.get_connection() as conn:
                return {row[0]: row[1] for row in conn.execute(sql).fetchall()}

        except sqlite3.Error as e:
            logging.error(f'ERROR: Failed to execute query sql:{sql}', e)

    def fetch_sum_of_all_tokens(self):
        sql = "select sum(frequency) from tokens t;"

        try:
            with self.get_connection() as conn:
                return [row[0] for row in conn.execute(sql).fetchall()]

        except sqlite3.Error as e:
            logging.error(f'ERROR: Failed to execute query sql:{sql}', e)

    # "token",
    # "frequency",
    # "probability",
    # "w_31", "p_31", "w_32", "p_32", "w_33", "p_33",
    # "w_51", "p_51", "w_52", "p_52", "w_53", "p_53", "w_54", "p_54", "w_55", "p_55"

    def update_token(self, token_update: list[dict]):
        sql = """update tokens set """
        for window in self.config.windows:
            for idx in range(1, window + 1):
                sql += f'w_{window}{idx} = :w_{window}{idx}, '
                sql += f'p_{window}{idx} = :p_{window}{idx},'
        sql = sql.removesuffix(",")
        sql += " where word = :word"

        try:
            with self.get_connection() as conn:
                conn.executemany(sql, token_update)

        except sqlite3.Error as e:
            print(f"ERROR: Failed to execute query sql:{sql}\ndata:{token_update}\ne: {e}")

def retry_on_failure(max_retries, delay=1):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for _ in range(max_retries):
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    print(f"Error occurred: {e}. Retrying...")
                    time.sleep(delay)
            raise Exception("Maximum retries exceeded. Function failed.")

        return wrapper
    return decorator