import pandas as pd
import ast


# yahoo_finance_suffix contains information about the construction of yahoo tickers.
# MIC contains information about stock exchanges
yahoo_df = pd.read_excel("yahoo_finance_suffix.xlsx", index_col=[0])
mic_df = pd.read_excel("MIC.xls")


# get suffix
def get_exchange_suffix_index(isin, yahoo_df, mic_df):
    # check whether isin is a symbol
    if len(isin) < 12:
        ideal_suffix = None
        ideal_exchange = None
        ideal_index = "^GSPC"
        return ideal_exchange, ideal_suffix, ideal_index
    country_code = isin[0:2]
    try:
        country_name = mic_df[
            mic_df["ISO COUNTRY CODE (ISO 3166)"] == country_code
        ].iloc[0]["COUNTRY"]
        exchange_df = yahoo_df[yahoo_df["Country"] == country_name]
        ideal_df = exchange_df[exchange_df["Major"] == 1]
        ideal_exchange = None
        ideal_suffix = None
        ideal_index = None
        if len(ideal_df) != 0:
            ideal_exchange = list(ideal_df["MIC"])[0]
            ideal_suffix = list(ideal_df["Suffix"])[0]
            ideal_index = list(ideal_df["Index"])[0]

    except:
        ideal_suffix = None
        ideal_exchange = None
        ideal_index = None
    return ideal_exchange, ideal_suffix, ideal_index


# build ticker
def get_ticker(symbol_list, exchange, suffix):
    if pd.isna(symbol_list):
        return None
    symbol_list = ast.literal_eval(symbol_list)

    if len(symbol_list) == 1 and symbol_list[0][0] is None:
        complete_symbol = symbol_list[0][1]
        return complete_symbol
    symbol = None

    for i in range(len(symbol_list)):
        if symbol_list[i][0] == exchange:
            symbol = symbol_list[i][1]
            # clean symbol
            if "." in symbol:
                symbol = symbol.split(".")[0]
            break

    if symbol:
        if pd.isna(suffix):
            complete_symbol = symbol
        else:
            complete_symbol = symbol + suffix
    else:
        complete_symbol = None
    return complete_symbol
