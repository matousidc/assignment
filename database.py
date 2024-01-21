import sqlite3
import pandas as pd


def db_interaction(conn: sqlite3.Connection, df_ips: pd.DataFrame, df_urls: pd.DataFrame, df_sources: pd.DataFrame):
    """All interactions with database"""
    with conn:
        # create tables
        create_table(conn, query_ips)
        create_table(conn, query_urls)
        create_table(conn, query_sources)
        # read db tables
        table_ips = pd.read_sql_query(f'SELECT * FROM IPs;', con=conn)
        table_urls = pd.read_sql_query(f'SELECT * FROM URLs;', con=conn)
        table_sources = pd.read_sql_query(f'SELECT * FROM sources;', con=conn)
        # find new rows
        if not table_urls.empty:
            df_ips = pd.concat([table_ips, df_ips]).drop_duplicates(keep=False)
            df_urls = pd.concat([table_urls, df_urls]).drop_duplicates(keep=False)
            df_sources = pd.concat([table_sources, df_sources]).drop_duplicates(keep=False)
        # insert into db
        df_ips.to_sql('IPs', conn, if_exists='append', index=False)
        df_urls.to_sql('URLs', conn, if_exists='append', index=False)
        df_sources.to_sql('sources', conn, if_exists='append', index=False)


def create_connection(db_file: str):
    """ create a database connection to the SQLite database specified by db_file"""
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Exception as exc:
        raise Exception("Connection failed") from exc


def create_table(conn: sqlite3.Connection, create_table_sql: str):
    """ create a table from the create_table_sql statement"""
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Exception as exc:
        raise Exception("table not created") from exc


query_sources = '''
    CREATE TABLE IF NOT EXISTS sources (
        domain TEXT NOT NULL,
        source TEXT NOT NULL
    );
'''

query_ips = """CREATE TABLE IF NOT EXISTS IPs (
    IP TEXT NOT NULL,
    col_1 INTEGER NOT NULL,
    col_2 INTEGER NOT NULL,
    col_3 TEXT,
    col_4 TEXT,
    col_5 TEXT,
    col_6 TEXT,
    col_7 INTEGER
);"""

query_urls = """CREATE TABLE IF NOT EXISTS URLs (
    id INTEGER,
    dateadded TEXT,
    url TEXT,
    url_status TEXT,
    last_online TEXT,
    threat TEXT,
    tags TEXT,
    urlhaus_link TEXT,
    reporter TEXT
);"""
