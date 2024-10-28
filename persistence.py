import sqlite3
from collections import Counter

from text_processor import Token

db_name = "alto_30_confidence.db"
token_stats_table = "token_stats"
token_stats_table_columns = ["id",
                             "token",
                             "decade",
                             "frequency",
                             "probability",
                             "w_31", "p_31", "w_32", "p_32", "w_33", "p_33",
                             "w_51", "p_51", "w_52", "p_52", "w_53", "p_53", "w_54", "p_54", "w_55", "p_55"]


def create_sqlite_database():
    """ create a database connection to an SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_name)
        print(sqlite3.sqlite_version)

        create_tables()
    except sqlite3.Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


def create_tables():
    print("create tables")
    sql_statements = [
        f"""CREATE TABLE IF NOT EXISTS {token_stats_table} (
                id INTEGER PRIMARY KEY, 
                token TEXT NOT NULL, 
                decade TEXT NOT NULL,
                frequency INT NOT NULL,
                probability REAL NOT NULL,
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
                p_55 REAL
        );""",
        """CREATE TABLE IF NOT EXISTS decade (
                id INTEGER PRIMARY KEY, 
                name TEXT NOT NULL
        );"""]

    # create a database connection
    try:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            for statement in sql_statements:
                cursor.execute(statement)

            conn.commit()
    except sqlite3.Error as e:
        print(e)


# "token",
# "decade",
# "frequency",
# "probability",
# "w_31", "p_31", "w_32", "p_32", "w_33", "p_33",
# "w_51", "p_51", "w_52", "p_52", "w_53", "p_53", "w_54", "p_54", "w_55", "p_55"

def add_token(decade: str, token: Token, probability: float):
    try:
        with sqlite3.connect(db_name) as conn:
            insert_columns = token_stats_table_columns[1:]
            vals = "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"

            sql = f"""INSERT INTO {token_stats_table}({",".join(insert_columns)})
                      VALUES({vals})"""
            cur = conn.cursor()

            empty_counter = Counter({'none':0})
            data = (token.token, decade, token.frequency, probability,

                    token.stats[3].distance_stat.get(1, empty_counter).most_common(1)[0][0],
                    token.stats[3].get_distance_prob()[1],
                    token.stats[3].distance_stat.get(2, empty_counter).most_common(1)[0][0],
                    token.stats[3].get_distance_prob()[2],
                    token.stats[3].distance_stat.get(3, empty_counter).most_common(1)[0][0],
                    token.stats[3].get_distance_prob()[3],

                    token.stats[5].distance_stat.get(1, empty_counter).most_common(1)[0][0],
                    token.stats[5].get_distance_prob()[1],
                    token.stats[5].distance_stat.get(2, empty_counter).most_common(1)[0][0],
                    token.stats[5].get_distance_prob()[2],
                    token.stats[5].distance_stat.get(3, empty_counter).most_common(1)[0][0],
                    token.stats[5].get_distance_prob()[3],
                    token.stats[5].distance_stat.get(4, empty_counter).most_common(1)[0][0],
                    token.stats[5].get_distance_prob()[4],
                    token.stats[5].distance_stat.get(5, empty_counter).most_common(1)[0][0],
                    token.stats[5].get_distance_prob()[5],
                    )

            cur.execute(sql, data)
            conn.commit()
        return cur.lastrowid
    except sqlite3.Error as e:
        print(f"ERROR: Failed to execute query sql:{sql}\ndata:{data}\ne: {e}")

