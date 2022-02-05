"""
This parser is designed for finanzen.net.
"""
import bs4
from newsplease import NewsPlease


def parse_html(html):
    """
    Parse html to find corresponding data fields.
    """
    soup = bs4.BeautifulSoup(html, features="lxml")
    ticker_name = None
    ticker_section = soup.find('div', {"class": "chart-block relative"})
    if ticker_section:
        ticker_name = ticker_section.find('a').get_text()

    article_time = None
    time_section = soup.find('div', {"class": "optionBar medium-font clearfix"})
    if time_section:
        article_time = time_section.text.strip()
        
    article = NewsPlease.from_html(html=html)
    date_publish = article.date_publish
    if not date_publish:
        date_publish = article_time

    text = article.maintext
    title = article.title
    return ticker_name, date_publish, text, title
