import time
import re
import requests
import pandas as pd
import urllib.parse
import bs4


headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

# base url for search companies (finanzen.net) 
finanzen_base_url = "https://www.finanzen.net/suchergebnis.asp?_search="


def get_wkn_isin(name, cache_df):
    """
    get wkn and isin from company name.
    """
    # lookup in the cache_df
    query_result = cache_df[cache_df['company_name']==name]
    if len(query_result) != 0:
        found = 'cache'
        return query_result.iloc[0]['company_name'], query_result.iloc[0]['wkn'], query_result.iloc[0]['isin'], cache_df, found
    
    url = finanzen_base_url + urllib.parse.quote_plus(name)
    print('send request...')
    response = requests.get(url, headers=headers)
    time.sleep(5)
    # get query result
    try_num = 1
    while response.status_code != 200:
        if try_num >= 6:
            company_name = None
            isin = None
            wkn = None
            found = 'query time out'
            return company_name, wkn, isin, cache_df, found
        # avoid hitting the server too hard
        sleep_time = 2**try_num
        time.sleep(sleep_time)
        response = requests.get(url)
        try_num += 1
    # parse query result
    soup = bs4.BeautifulSoup(response.text, features="lxml")
    query_section = soup.find("div", {"class": "table-responsive"})
    if query_section:
        company_link = query_section.find('a')
        if company_link:
            company_name = company_link.text
            isin = company_link.find_next('td').text
            wkn = company_link.find_next('td').find_next('td').text
            if company_name == name:
                found = 'query right'
            else:
                found = 'query wrong'
                
        else:
            company_name = None
            isin = None
            wkn = None
            found = 'query zero'

    else:
        company_name = None
        isin = None
        wkn = None
        found = 'query no'

    cache_df = cache_df.append(pd.Series([company_name, wkn, isin], index = cache_df.columns), ignore_index=True)
    cache_df = cache_df.dropna(subset=['company_name'])

    return company_name, wkn, isin, cache_df, found


def find_yahoo_ticker_from_html(html):
    ticker_list = re.findall('ticker=[^A-Za-z0-9]*[A-Za-z0-9]*[^A-Za-z0-9]*[A-Za-z0-9]*', html)
    ticker_list = [ticker.split('"')[1].replace("\\", "") for ticker in ticker_list]
    return ";".join(list(set(ticker_list))) if len(ticker_list) != 0 else None


def find_yahoo_ticker_from_title(title):
    full_ticker_list = []
    exchange_suffix_dic = {"LON": ".L",
                            "NYSE": "",
                            "NASDAQ":"",
                            "FRA": ".F",
                            "TSE": ".TO",
                            "CVE": ".V",
                            "ASX": ".AX",
                            "NZSE": ".NZ",
                            "VTX": ".SW",
                            "AMS": ".AS",
                            "MUN": ".MU",
                            "ETR": ".DE",
                            "AMEX": "",
                            "KLSE": ".KL",
                            "JSE": ".JO",
                            "SGX": ".SI"
                            }
    
    ticker_info_list = re.findall('\(.*?\)', title) 
    ticker_info_list = [x.replace("(", "").replace(")", "") for x in ticker_info_list]
    ticker_info_list = [x for x in ticker_info_list if x.isupper()]

    if len(ticker_info_list) != 0:
        for ticker_info in ticker_info_list:
            if ":" in ticker_info:
                exchange, ticker = ticker_info.split(":")
                ticker = ticker.replace(".", "")
                if exchange in exchange_suffix_dic.keys():
                    suffix = exchange_suffix_dic[exchange]
                    full_ticker = ticker + suffix
                    full_ticker_list.append(full_ticker)
                else:
                    print("Warning: there is no exchange info for {}".format(ticker_info))
            else:
                full_ticker = ticker_info
                full_ticker_list.append(full_ticker)

    return ";".join(list(set(full_ticker_list))) if len(full_ticker_list) != 0 else None


def find_seekalpha_ticker(html_content):
    ticker_candidates = re.findall('<a href="[^"]*" title', html_content)
    ticker_candidates = [ticker.split('"')[1].split("seekingalpha.com/symbol/")[1] for ticker in ticker_candidates if "seekingalpha.com/symbol/" in ticker and '/' not in ticker]
    return ";".join(set(ticker_candidates))
