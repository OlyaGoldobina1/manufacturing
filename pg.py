import pandas as pd
import psycopg2
import psycopg2.extras as extras
import io


def insert_table(df: pd.DataFrame, table: str, login: dict) -> None:

    conn = psycopg2.connect(**login)
    conn.autocommit = False
    cursor = conn.cursor()

    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ', '.join(list(df.columns))

    query = f'INSERT INTO {table} ({cols}) VALUES %s'
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except(Exception, psycopg2.DatabaseError) as e:
        conn.rollback()
    cursor.close()
    conn.close()


def query_to_df(query: str, login: dict) -> pd.DataFrame:

    conn = psycopg2.connect(**login)
    conn.autocommit = False
    cursor = conn.cursor()

    buf = io.StringIO()

    cursor.copy_expert(f'COPY ({query}) TO STDOUT WITH CSV HEADER', buf)

    buf.seek(0)

    df = pd.read_csv(buf)

    conn.close()
    cursor.close()

    return df