import requests
import pandas as pd
import urllib.parse
import time
import bs4


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
}


# base url for search companies (finanzen.net)
# note: this is only for illustration purpose and should not be used against the mentioned website
finanzen_base_url = "https://www.finanzen.net/suchergebnis.asp?_search="


def get_wkn_isin(name, cache_df):
    """
    get wkn and isin from company name.
    note that we cache the query result so that we don't need to send request everytime.
    """
    # lookup in the cache_df
    query_result = cache_df[cache_df["company_name"] == name]
    if len(query_result) != 0:
        found = "cache"
        return (
            query_result.iloc[0]["company_name"],
            query_result.iloc[0]["wkn"],
            query_result.iloc[0]["isin"],
            cache_df,
            found,
        )

    url = finanzen_base_url + urllib.parse.quote_plus(name)
    print("send request...")
    response = requests.get(url, headers=HEADERS)
    time.sleep(5)
    # get query result
    company_name = None
    isin = None
    wkn = None
    try_num = 1
    while response.status_code != 200:
        # we try 7 times before we quit
        if try_num >= 6:
            found = "query time out"
            return company_name, wkn, isin, cache_df, found
        # avoid hitting the server too hard
        sleep_time = 2 ** try_num
        time.sleep(sleep_time)
        response = requests.get(url)
        try_num += 1
    # parse query result
    soup = bs4.BeautifulSoup(response.text, features="lxml")
    query_section = soup.find("div", {"class": "table-responsive"})
    if query_section:
        company_link = query_section.find("a")
        if company_link:
            company_name = company_link.text
            isin = company_link.find_next("td").text
            wkn = company_link.find_next("td").find_next("td").text
            if company_name == name:
                found = "query right"
            else:
                found = "query wrong"
        else:
            found = "query zero"
    else:
        found = "query no"

    cache_df = cache_df.append(
        pd.Series([company_name, wkn, isin], index=cache_df.columns), ignore_index=True
    )
    cache_df = cache_df.dropna(subset=["company_name"])

    return company_name, wkn, isin, cache_df, found
