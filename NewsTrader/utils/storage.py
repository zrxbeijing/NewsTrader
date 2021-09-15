import sqlite3
import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PriceData:
    """
    Write and read stock price data in sqlite.
    """

    def __init__(self, data_dir, sqlite_file, chunk_size: int = 500):
        self.data_dir = data_dir
        self.sqlite_file = sqlite_file
        self.chunk_size = chunk_size

    def write_sqlite(self, data_df):
        """
        Write data into sqlite.
        :param data_df: new data_df
        :return: None
        """
        # before we write into the disk, we should check whether the database exists
        if os.path.exists(os.path.join(self.data_dir, self.sqlite_file)):
            logger.info("There is already a database! We remove it and store new data!")
            os.remove(os.path.join(self.data_dir, self.sqlite_file))
        ticker_df = data_df.drop(columns=["Date"])
        chunks = [
            ticker_df.iloc[:, x : x + self.chunk_size]
            for x in range(0, len(ticker_df.columns), self.chunk_size)
        ]
        chunks = [
            data_df[["Date"]].merge(chunk, left_index=True, right_index=True)
            for chunk in chunks
        ]
        for i in range(len(chunks)):
            logger.info(
                "Writing no.{} table in total of {} tables".format(
                    str(i), str(len(chunks))
                )
            )
            con = sqlite3.connect(os.path.join(self.data_dir, self.sqlite_file))
            chunk = chunks[i]
            chunk.to_sql(
                "table_{}".format(str(i)), con=con, if_exists="append", index=False
            )

    def read_sqlite(self):
        """
        Read data from sqlite database
        :return: data_df
        """
        con = sqlite3.connect(os.path.join(self.data_dir, self.sqlite_file))
        cursor = con.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [table[0] for table in cursor.fetchall()]
        df_list = []
        for table in tables:
            logger.info(
                "Reading {} in total of {} tables".format(table, str(len(tables)))
            )
            query = "SELECT * FROM {}".format(table)
            df = pd.read_sql_query(query, con, parse_dates=["Date"])
            df.index = df["Date"]
            df.drop(columns=["Date"], inplace=True)
            df_list.append(df)
        data_df = pd.concat(df_list, axis=1)
        data_df.reset_index(inplace=True)
        return data_df


class NewsData:
    """
    Write and read news data in sqlite.
    """

    def __init__(self, data_dir, sqlite_file):
        self.data_dir = data_dir
        self.sqlite_file = sqlite_file

    def write_sqlite(self, news_df):
        con = sqlite3.connect(os.path.join(self.data_dir, self.sqlite_file))
        news_df.to_sql("table_1", con=con, if_exists="append", index=False)

    def read_sqlite(self):
        con = sqlite3.connect(os.path.join(self.data_dir, self.sqlite_file))
        news_df = pd.read_sql_query("SELECT * FROM table_1", con)
        return news_df
