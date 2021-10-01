# fetch stock and index price data for event study
# last edit date: 2021-03-19
from math import exp
from pandas import to_datetime
from pandas_datareader import get_data_yahoo
from time import sleep


class PriceFetcher(object):
    """
    This class implements how to fecht stock price data and corresponding index price data for event study.
    We have two ways of fetching data: downloading internet data or loading local data.
    To process local stored data, set data_df to the DataFrame.
    Otherwise we extract stock data from Yahoo Finance.
    """

    def __init__(
        self,
        ticker=None,
        stock_index=None,
        start_date=None,
        end_date=None,
        data_df=None,
        date_column="Date",
    ):
        """
        This is a price fetcher.
        :param ticker: Stock ticker.
        :param stock_index: Stock index, usually the most representative stock index
        :param start_date: The begin of the observation period. Pattern: "%Y-%m-%d"
        :param end_date: The end of the observation period. Pattern: "%Y-%m-%d"
        :param data_df: the local stock and index price data. data_df.column = [self.date_column, self.ticker,
        self.stock_index]
        :param date_column: the name of the column containing date information.
        """
        self.ticker = ticker
        self.stock_index = stock_index
        self.start_date = to_datetime(start_date)
        self.end_date = to_datetime(end_date)
        self.data_df = data_df
        self.date_column = date_column

    def fetch(self):
        """
        Fetch the corresponding stock price data. If the data doesn't exist, the download method will be initiated.
        :return: stock price data in the form of DataFrame
        """
        if self.data_df is not None:
            # if local data is provided
            stock_df = self.data_df[[self.date_column] + [self.ticker]]
            index_df = self.data_df[[self.date_column] + [self.stock_index]]
        else:
            # if not, download from Yahoo Finance
            stock_df = self._download(self.ticker)
            index_df = self._download(self.stock_index)
        if stock_df is None or index_df is None:
            return None
        # merge the stock and index price data in a DataFrame
        price_df = stock_df.merge(index_df, on=self.date_column)
        # adjust the start date and end date, because they could be somewhat different from what we input.
        start_date = self.start_date
        end_date = self.end_date
        if (self.start_date - price_df.iloc[0][self.date_column]).days < 0:
            start_date = price_df.iloc[0][self.date_column]
        if (self.end_date - price_df.iloc[-1][self.date_column]).days > 0:
            end_date = price_df.iloc[-1][self.date_column]
        indicators = price_df[self.date_column].apply(
            lambda x: True
            if (x - start_date).days >= 0 and (x - end_date).days <= 0
            else False
        )
        price_df = price_df.loc[indicators, :]
        price_df = price_df.dropna().reset_index(drop=True)
        return price_df

    def _download(self, symbol):
        """
        Download stock price data from open source datasets such as Yahoo finance.
        Here we create a mechanism to avoid being annoyed by the limitation set by Yahoo finance.
        :param symbol: stock symbol on Yahoo Finance
        :return: DataFrame. Price data for the stock.
        """
        yahoo_df = None
        while yahoo_df is None:
            try:
                yahoo_df = get_data_yahoo(
                    symbols=symbol, start=self.start_date, end=self.end_date
                )
            except KeyError:
                # the symbol is wrong or there is no data for this.
                print("There is no data for the symbol {}".format(self.ticker))
                break
            except Exception as ex:
                # avoid DDOS attack
                print(ex)
                break
        if yahoo_df is None:
            return None
        # after successful retrieval of information from yahoo finance, we convert the format of returned
        # DataFrame to what we desire.
        yahoo_df[self.date_column] = to_datetime(yahoo_df.index)
        yahoo_df = yahoo_df.reset_index(drop=True)
        yahoo_df = yahoo_df[[self.date_column, "Adj Close"]]
        yahoo_df.columns = [self.date_column, symbol]

        return yahoo_df
