import requests
from bs4 import BeautifulSoup


def isin_to_ticker(isin):
    if not isinstance(isin, str):
        return None
    if len(isin) < 12:
        ticker_result = [(None, isin)]
        return ticker_result
    if "US" in isin:
        prefix = "https://www.morningstar.com/search/us-securities?query="
        select_class = "search-us-securities"
    else:
        prefix = "https://www.morningstar.com/search/foreign-securities?query="
        select_class = "search-foreign-securities"

    url = prefix + isin
    r = requests.get(url)
    if r.status_code != 200:
        print("morning star server does not response correctly")
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    result = soup.find("section", {"class": select_class})
    results = result.find_all("div", {"class": "mdc-security-module__metadata"})

    # check whether result is None
    if len(results) == 0:
        return None
    exchanges = [result.find_all("span")[0].getText() for result in results]
    symbols = [result.find_all("span")[1].getText() for result in results]
    ticker_result = list(zip(exchanges, symbols))

    return ticker_result
