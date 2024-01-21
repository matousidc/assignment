import unittest
import pandas as pd
from scrape_data import load_alienvault, load_openphish, create_dfs


class TestLoadAlienvault(unittest.TestCase):
    def test_correct_number_of_columns(self):
        # Call the function to load the Alienvault data
        df = load_alienvault()

        # Check if the number of columns is correct
        expected_columns = ['IP', 'col_1', 'col_2', 'col_3', 'col_4', 'col_5', 'col_6', 'col_7']
        self.assertEqual(list(df.columns), expected_columns, "Incorrect number or names of columns")


class TestLoadOpenphish(unittest.TestCase):
    def test_correct_number_of_columns(self):
        df = load_openphish()
        self.assertEqual(df.shape[1], 1, "Incorrect number of columns")


class TestCreateDfs(unittest.TestCase):
    def test_correct_number_of_rows(self):
        df_ips, df_urls, df_sources = create_dfs()
        self.assertEqual(df_ips.shape[0] + df_urls.shape[0], df_sources.shape[0], "Incorrect number of rows")


if __name__ == '__main__':
    unittest.main()
