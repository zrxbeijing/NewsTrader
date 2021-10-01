# This module aims to wrap the event study sub-package, and provide the functionality of multiprocessing

import pandas as pd
from functools import partial
from multiprocessing import cpu_count
from .return_calculator import EventReturnCalculator
from NewsTrader.utils.storage import PriceData
from NewsTrader.utils.accelerator import run_multitasking


def event_study(
    df,
    date_column=None,
    ticker_column=None,
    stock_index_column=None,
    window_size=None,
    window_distance=None,
    estimation_period=None,
    log_mode=False,
    data_dir="quant/dataset",
    sqlite_file="stock_price.db",
    num_process=None,
):
    """
    A wrapper function to be called by user to conduct event study.
    :param df:
    :param date_column:
    :param ticker_column:
    :param stock_index_column:
    :param window_size:
    :param window_distance:
    :param estimation_period:
    :param log_mode:
    :param data_dir:
    :param sqlite_file:
    :param num_process:
    :return:
    """
    # we load first the data price data in data_df
    if sqlite_file:
        data_df = PriceData(data_dir, sqlite_file).read_sqlite()
    else:
        data_df = None
    if not event_study_check(df, data_df, ticker_column, stock_index_column):
        print("data preparation is not finished!")
        return
    if data_df is None:
        # if we need to download data from yahoo finance, better use single process
        num_process = 1
    result = multi_event_study(
        df,
        data_df,
        date_column,
        ticker_column,
        stock_index_column,
        window_size,
        window_distance,
        estimation_period,
        log_mode,
        num_process,
    )

    return result


def event_study_check(
    df=None, data_df=None, ticker_column=None, stock_index_column=None
):
    """
    Check wheter the requirments are fulfilled before event study.
    :param df: event data DataFrame
    :param data_df: price data DataFrame
    :param ticker_column: ticker column
    :param stock_index_column: stock index column
    :return: whether we can start event study
    """
    if not data_df:
        return True
    ticker_set = set(df[ticker_column])
    index_set = set(df[stock_index_column])
    ticker_index_set = ticker_set.union(index_set)
    data_symbol_set = set(data_df.columns)
    if ticker_index_set <= data_symbol_set:
        return True
    else:
        return False


def multi_event_study(
    df,
    data_df,
    date_column,
    ticker_column,
    stock_index_column,
    window_size,
    window_distance,
    estimation_period,
    log_mode,
    num_process,
):
    """
    Conduct event study by leverage the power of multiprocessing.
    :param df: input DataFrame containing event timestamp and ticker
    :param data_df: input DataFrame containing stock price data
    :param date_column:
    :param ticker_column:
    :param stock_index_column: the stock index used for the market model
    :param window_size: the size of the event window
    :param window_distance: how long is the distance between eåvent window and estimation window
    :param estimation_period: how long is the estimation period
    :param log_mode: whether log mode should be use during return calculation
    :param num_process: number of processes
    :return: pd.DataFrame for event study result
    """
    if num_process is None:
        worker_num = 1
    else:
        if isinstance(num_process, int):
            worker_num = num_process
        else:
            worker_num = cpu_count() - 1 if cpu_count() > 1 else 1
    # two dynamic parameters: event and price_df
    event_list = [event for index, event in df.iterrows()]
    if data_df:
        price_df_list = [
            data_df[[date_column] + [event["ticker"], event[stock_index_column]]]
            for event in event_list
        ]
    else:
        price_df_list = [None for event in event_list]
    argument_list = zip(event_list, price_df_list)
    partial_func = partial(
        one_event_study,
        date_column=date_column,
        ticker_column=ticker_column,
        stock_index_column=stock_index_column,
        window_size=window_size,
        window_distance=window_distance,
        estimation_period=estimation_period,
        log_mode=log_mode,
    )

    result_list = run_multitasking(
        func=partial_func,
        argument_list=argument_list,
        num_workers=worker_num,
        thread_or_process="process",
    )

    # prepare final output
    date_column_list = [
        "date t{}".format(str(day))
        for day in list(range(-window_size, window_size + 1))
    ]
    return_column_list = [
        "return t{}".format(str(day))
        for day in list(range(-window_size, window_size + 1))
    ]
    abnormal_column_list = [
        "ab return t{}".format(str(day))
        for day in list(range(-window_size, window_size + 1))
    ]
    series_index = (
        date_column_list
        + return_column_list
        + abnormal_column_list
        + ["rsqured", "intercept", "beta", "residual"]
    )
    final_result_list = []
    for k in range(len(result_list)):
        window_df = result_list[k]
        if window_df is not None and len(window_df) == 2 * window_size + 1:
            result = pd.Series(
                index=series_index,
                data=list(window_df[date_column])
                + list(window_df["stock_return"])
                + list(window_df["abnormal_return"])
                + [window_df["rsqured"][0]]
                + [window_df["intercept"][0]]
                + [window_df["beta"][0]]
                + [window_df["residual"][0]],
            )
        else:
            result = pd.Series(
                index=series_index, data=[None for i in range(len(series_index))]
            )
        final_result_list.append(result)
    empirical_result = pd.concat(final_result_list, axis=1).transpose()
    combined_result = df.merge(empirical_result, left_index=True, right_index=True)
    return combined_result


def one_event_study(
    event,
    price_df,
    date_column,
    ticker_column,
    stock_index_column,
    window_size,
    window_distance,
    estimation_period,
    log_mode,
):
    """
    Undertake event study for one event
    :param event:
    :param price_df:
    :param date_column:
    :param ticker_column:
    :param stock_index_column:
    :param window_size:
    :param window_distance:
    :param estimation_period:
    :param log_mode:
    :return: pd.Series
    """
    ticker, event_date, stock_index = (
        event[ticker_column],
        event[date_column],
        event[stock_index_column],
    )
    window_df = EventReturnCalculator(
        ticker=ticker,
        event_date=event_date,
        stock_index=stock_index,
        data_df=price_df,
        log_mode=log_mode,
        date_column=date_column,
    ).calculate_window_abnormal(window_size, window_distance, estimation_period)
    return window_df
