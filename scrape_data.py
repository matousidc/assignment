import pandas as pd
import requests


def pandas_show_options(rows=None, columns=None, width=None):
    """Sets params for printing df"""
    if rows:
        pd.set_option('display.max_rows', rows)
    if columns:
        pd.set_option('display.max_columns', columns)
    if width:
        pd.set_option('display.width', width)


def load_urlhaus() -> pd.DataFrame:
    """Load data in dataframe"""
    url = "https://urlhaus.abuse.ch/downloads/csv_recent/"
    df = pd.read_csv(url, skiprows=8, delimiter=",")
    df.rename(columns={"# id": "id"}, inplace=True)
    return df


def load_openphish() -> pd.DataFrame:
    """Load data in dataframe"""
    url = "https://openphish.com/feed.txt"
    response = requests.get(url)
    data = response.text
    domains = data.split(sep="\n")[:-1]
    df = pd.DataFrame({'url': domains})
    return df


def load_alienvault() -> pd.DataFrame:
    """Load data in dataframe"""
    url = "http://reputation.alienvault.com/reputation.data"
    df = pd.read_csv(url, sep="#", header=None,
                     names=['col_0', 'col_1', 'col_2', 'col_3', 'col_4', 'col_5', 'col_6', 'col_7'])
    df.rename(columns={'col_0': "IP"}, inplace=True)
    return df


def domain_sources(df_urlhaus: pd.DataFrame, df_openphish: pd.DataFrame, df_ips: pd.DataFrame) -> pd.DataFrame:
    """Dataframe with info about origin of IOCs"""
    df1 = pd.DataFrame({'domain': df_urlhaus["url"], "source": "urlhaus"})
    df2 = pd.DataFrame({"domain": df_ips["IP"], "source": "alienvault"})
    df3 = pd.DataFrame({"domain": df_openphish["url"], "source": "openphish"})
    result_df = pd.concat([df1, df2, df3], ignore_index=True)
    return result_df


def create_dfs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Prepares dataframes that will be inserted into db tables"""
    pandas_show_options(columns=9, width=1000)
    df_urlhaus = load_urlhaus()
    df_openphish = load_openphish()
    # dfs as tables
    df_ips = load_alienvault()
    df_urls = pd.concat([df_urlhaus, df_openphish], ignore_index=True)
    df_sources = domain_sources(df_urlhaus, df_openphish, df_ips)
    return df_ips, df_urls, df_sources
