from scrape_data import create_dfs
from database import create_connection, db_interaction


def main():
    df_ips, df_urls, df_sources = create_dfs()
    conn = create_connection(db_file="prod_db.db")
    db_interaction(conn, df_ips, df_urls, df_sources)


if __name__ == "__main__":
    main()
