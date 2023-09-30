# This module is used for calculating window (abnormal) normal returns.

from .price_fetcher import PriceFetcher
from statsmodels.formula.api import ols
import pandas as pd
import numpy as np


class EventReturnCalculator(object):
    def __init__(self,
                 ticker=None,
                 event_date=None,
                 stock_index=None,
                 data_df=None,
                 log_mode=True,
                 date_column='Date'):
        """
        This is a return calculator, designed specially for conducting event study.
        :param ticker: the stock ticker.
        :param event_date: the event date, should be in format of "%Y-%m-%d"
        :param stock_index: the ticker of the stock index
        :param data_df: local data_df containing stock and index price data.
        :param log_mode: whether log mode should be implemented
        :param date_column: the name of the column containing date information
        """
        self.data_df = data_df
        self.ticker = ticker
        self.event_date = pd.to_datetime(event_date).normalize()
        self.stock_index = stock_index
        self.log_mode = log_mode
        self.date_column = date_column

    def _calculate_return(self, start_date, end_date):
        """
        A simple return calculator for event study.
        :param start_date: the beginning date of the calculation.
        :param end_date: the ending date of the calculation.
        :return: a return DataFrame.
        """
        price_df = PriceFetcher(
            self.ticker, self.stock_index, start_date, end_date, self.data_df, self.date_column).fetch()
        if price_df is None:
            return None
        price_df['lag_stock'] = price_df[self.ticker].shift(periods=1)
        price_df['lag_index'] = price_df[self.stock_index].shift(periods=1)
        if self.log_mode:
            price_df['stock_return'] = np.log(price_df[self.ticker]) - np.log(price_df['lag_stock'])
            price_df['index_return'] = np.log(price_df[self.stock_index]) - np.log(price_df['lag_index'])
        else:
            price_df['stock_return'] = (price_df[self.ticker] - price_df['lag_stock']) / price_df['lag_stock']
            price_df['index_return'] = (price_df[self.stock_index] - price_df['lag_index']) / price_df['lag_index']
        price_df.dropna(subset=['stock_return', 'index_return'], inplace=True)
        price_df.reset_index(level=0, inplace=True)
        return price_df[[self.date_column, 'stock_return', 'index_return']]

    def _create_intermediate(self, return_df=None):
        """
        Create a intermediate df to deal with non-trading days, in order to create a final window return df.
        :param return_df: a DataFrame containing return data for target stock and index.
        :return: an intermediate df
        """
        # create a df_template, which does not regard whether there is non-trading day
        df_template = pd.DataFrame(
            index=range((return_df[self.date_column].iloc[-1] - return_df[self.date_column].iloc[0]).days + 1),
            columns=return_df.columns)
        df_template[self.date_column] = [return_df[self.date_column].iloc[0] + pd.Timedelta(days=i)
                               for i in range(
                (return_df[self.date_column].iloc[-1] - return_df[self.date_column].iloc[0]).days + 1)]
        df_template['drift'] = df_template[self.date_column] - self.event_date
        df_template['drift'] = df_template['drift'].apply(lambda x: x.days)
        # concatenate return_df and df_template: return_df comes before df_template
        concat_df = pd.concat([return_df, df_template])
        # if there is duplicated rows, we keep those in return_df first. Otherwise we choose those in df_template
        concat_df = concat_df.drop_duplicates(subset=['drift'], keep='first')
        # sort concat_df according to drift to see clearly whether data points are from trading or non-trading days
        concat_df = concat_df.sort_values(by='drift').reset_index(drop=True)
        # finally we remove non-trading days
        intermediate_df = concat_df.dropna().reset_index(drop=True)
        return intermediate_df

    def _find_window_return(self, return_df=None, window_size=None):
        """
        Find window returns from raw return DataFrame.
        :param return_df: a DataFrame containing return data for target stock and index.
        :param window_size: the size of the event window.
        :return: well-organized window return DataFrame
        """
        # add a 'drift' column, recording the distance between the event day and each observation day.

        return_df['drift'] = return_df[self.date_column] - self.event_date
        return_df['drift'] = return_df['drift'].apply(lambda x: x.days)
        # create a template DataFrame.
        intermediate_df = self._create_intermediate(return_df)
        window_list = list(range(-window_size, window_size + 1))
        if len(intermediate_df[intermediate_df['drift'] >= 0]) == 0:
            return None
        zero_index = intermediate_df[intermediate_df['drift'] >= 0].index[0]
        # drop old calendar-day drift
        intermediate_df = intermediate_df.drop(columns=['drift'])
        # set new trading-day drift
        drift_trading = [index - zero_index for index, row in intermediate_df.iterrows()]
        intermediate_df['drift'] = drift_trading
        # set trading_day drift as index
        window_return_df = intermediate_df[
            intermediate_df['drift'].isin(window_list)].reset_index(drop=True)[
            ['drift', self.date_column, 'stock_return', 'index_return']].set_index('drift')
        return window_return_df

    def _calculate_window_return(self, window_size=None, date_range_multiplier=7):
        """
        Calculate the absolute returns during the event window.
        :param window_size: the size of the event window.
        :param date_range_multiplier: to get enough data for corresponding window size, we can expand the date range to
        some extent (to take no-trading day into consideration)
        :return: a dict containing the absolute returns during the event window.
        """
        # To make sure we have stock price data covering the whole event window, we set the start_date and end_date for
        # requiring price data as window_size * 7. This would not be a problem, as the PriceFetcher will find exactly
        # return the best possible data for us, and the window return calculation process will choose the right window
        # range for the event study.
        start_date = self.event_date - pd.Timedelta(days=window_size * date_range_multiplier)
        end_date = self.event_date + pd.Timedelta(days=window_size * date_range_multiplier)
        return_df = self._calculate_return(start_date, end_date)
        if return_df is None or len(return_df) == 0:
            return None
        window_return_df = self._find_window_return(return_df, window_size)
        return window_return_df

    def _estimate_market_model(self, window_distance=None, period_len=None, date_range_multiplier=2):
        """
        Estimate the market model.
        :param window_distance: how many days earlier than the begin of the event window.
        :param period_len: how long is the estimation period.

        :return: a dict containing the abnormal returns during the event window.
        """
        end_date = self.event_date - pd.Timedelta(days=window_distance)
        # start_date should be early enough to get the length of period_len. Here we multiply period_len by 2.
        start_date = end_date - pd.Timedelta(days=period_len * date_range_multiplier)
        return_df = self._calculate_return(start_date, end_date)
        if return_df is None or len(return_df) < period_len:
            # if there is no enough data, return None
            intercept, beta, rsquared, residual = None, None, None, None
        else:
            return_df = return_df.iloc[-period_len:]
            regression_result = ols(formula="stock_return ~ index_return", data=return_df).fit()
            intercept = regression_result.params['Intercept']
            beta = regression_result.params['index_return']
            rsquared = regression_result.rsquared
            residual = list(regression_result.resid)
        return intercept, beta, rsquared, residual

    def calculate_window_abnormal(self, window_size=None, window_distance=None, period_len=None):
        """
        Calculate the abnormal return.
        :param window_size: the size of the event window.
        :param window_distance: how many days earlier than the begin of the event window.
        :param period_len: how long is the estimation period.
        :return: a dict containing the abnormal returns during the event window.
        """
        window_return_df = self._calculate_window_return(window_size)
        intercept, beta, rsquared, residual = self._estimate_market_model(window_distance, period_len)
        if window_return_df is None or rsquared is None:
            return None
        window_return_df['abnormal_return'] = window_return_df['stock_return'] - intercept - beta*window_return_df[
            'index_return']
        window_return_df['rsqured'] = [rsquared for i in range(len(window_return_df))]
        window_return_df['residual'] = [residual for i in range(len(window_return_df))]
        window_return_df['intercept'] = [intercept for i in range(len(window_return_df))]
        window_return_df['beta'] = [beta for i in range(len(window_return_df))]
        return window_return_df
