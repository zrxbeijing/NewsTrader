# NewsTrader
Rongxin Zhang

## Introduction
_NewsTrade_ is a Python package for news trading.
In the internet era, it is impossible to ignore the overwhelming presence of online news.
New information regarding listed companies contained in news can be digested by the market in a lightning speed.
Despite its importance, the mining of valuable information in news is still at its infancy, at least for individuals.
The access to massive news data, the processing of unstructured textual data and the modeling methodology pose great 
challenges for those who have the similar idea to exploit news to make insightful investment decisions.

This package is based on the development in NLP in recent years and aimed to apply it into daily trading by analyzing 
real-time financial news.
Many key components of this package are based on some awesome open-source packages.
However, We do not find any Python package which can fulfill the task of news trading in practice for private investors.
We try to propose a universe framework to do news trading, by gluing all necessary components together.
Note that this project is under construction and may only serve as an illustrative example.


## What is news trading
The idea of news trading is intuitive: the trading decisions are made based on relevant and fresh financial news.
To achieve this goal, we need a complete framework covering the process of fetching news and stock data, pre-processing 
data, training language models, constructing portfolios and back-testing strategy performance.

## Major Components
### News Data Sources
Modules: NewsTrader.newsfeed
- NewsTrader.newsfeed.commoncrawldatabase
- NewsTrader.newsfeed.gdeltdatabase
- NewsTrader.newsfeed.gegdatabase
- NewsTrader.newsfeed.downloader

News are prevailing on internet.
For investors company news are of great importance.
However, the fact that there are various news sources makes it difficult for retail investors to have an overall and
complete vision for stocks.
To have a quantitative and complete view of the stock market, it is required that: 
- investors have access to as many news sources as possible;
- ability to extract insightful information from them.

For this news trading project, we need in general two types of data: news data and stock data.
The first type of data is news data.
In general, we have two ways of obtaining news data.
The first one is to extract news data from open-source datasets, such as _GDELT_ and _CommonCrawl_.
These datasets aim to democratize the access to news and have gain attention from the investment community.
#### Gdelt databases
The GDELT Project claims to be the largest open-source news database.
There is already a python package called __gdelt__, which facilitates the extraction of Gdelt databases.
To simplify the usage, we wrap the main functionality of __gdelt__ into a query class.

```
from NewsTrader.newsfeed.gdeltdatabase import GdeltDatabase


# three sub databases are available: events, gkg, and mentions
gdelt_data = GdeltDatabase(date="2021-09-01", table="events").query()
gdelt_data.head()
```

Speically, there is another sub news database of the GDELT Project, called Global Entity Graph, in which news are already processed by Google.
However, there is no corresponding library which can easily extract information from this sub database.
In this project, we provide a convenient tool to get news data from it.

```
from NewsTrader.newsfeed.gegdatabase import GegDatabase


geg_data = GegDatabase(start="2021-09-01", end="2021-09-01", num_process=8).get_table()
geg_data.head()

```

#### CommonCrawl news database
The CommonCrawl provides tons of scrapped webpages and served as an important source of internet data.
We focus explicitly on the CC news database of the CommonCrawl Project, as we are only interested in news articles, but not general webpages.
We showcase how to dive into these datasets and extract useful text data for the purpose of use in building language
models by leveraging the library __newsplease__.

```
from NewsTrader.newsfeed.commoncrawldatabase import CCnewsDatabase, get_news_from_domain


cc_news_database = CCnewsDatabase(date_string='2020-09-01 12:00:00', only_allowed_domains=True, allowed_domains=['marketwatch.com'])
cc_news = get_news_from_domain(cc_news_database)

```

#### General news scrapying
If the news article data is not available (in a open-source database), an alternative way is to scrape down news articles from websites in a polite and gentle way, via fast scraping methods such as _scrapy_. 
We try to build a very simple scrapy framework to get historical or live news from major news websites in a gentle manner.
Besides scraping down html pages, another major challenge is to parse html pages from various sources in a structured way.
Thanks to the comprehensive extractor module of the wonderfull package _newsplease_, we can extract useful news information such as news title, news main text and news publish date in a relatively efficient way.

```
from NewsTrader.newsfeed.gdeltdatabase import GdeltDatabase
from NewsTrader.newsfeed.downloader import get_articles


# given urls, get the articles from the servers
gdelt_data = GdeltDatabase(date="2021-09-01", table="events").query()
urls = [url for url in list(gdelt_data.loc[0:100]['SOURCEURL']) if type(url) is str]
articles = get_articles(urls)

```

### News feature extraction
This project provides methods to extract features from news articles. 
#### Extract titles from given urls
Sometimes news articles are not available, but urls are provided.
Many news articles are assigned with a url in which the title information is embedded.
We make use of this point to extract title information from urls to do quick check of the news article.
For quick screening of articles, we don't need to scrape down the original articles from servers.

```
from NewsTrader.newsfeed.gdeltdatabase import GdeltDatabase
from NewsTrader.newsfeature import extract_title

df = GdeltDatabase(date='2021-09-14', table='events').query()

df['title'] = extract_title(list(df.SOURCEURL), mode='url', check_english=False, num_process=16)
df = df.dropna(subset=['title']).reset_index(drop=True)


```

#### Extract symbols
We can extract symbols from sentences if any listed stock is mentioned in sentences.
This is a very important feature of this project that can link news articles with specific companies and thus enables the news trading idea.
The basic idea behind this feature is that we first pick up organization names by applying NER function of the BERT model and then compare their word embeddings with those of tickers in the ticker database.


```
from NewsTrader.newsfeed.gdeltdatabase import GdeltDatabase
from NewsTrader.newsfeature import extract_title, extract_symbols

df = GdeltDatabase(date='2021-10-01', table='events').query()

df['title'] = extract_title(list(df.SOURCEURL), mode='url', check_english=False, num_process=16)
df = df.dropna(subset=['title']).reset_index(drop=True)

titles = list(df.title)
result = extract_symbols(titles,
                         min_score=0.65, 
                         min_similarity=0.80, 
                         only_preferred=False)

df['possible_symbols'] = [None if candidate_list is None else [candidate['symbol'] for candidate in candidate_list] for candidate_list in result]
df['company_names'] = [None if candidate_list is None else [candidate['long_name'] for candidate in candidate_list] for candidate_list in result]
df = df[['SOURCEURL', 'title',
       'possible_symbols', 'company_names']]

df = df.dropna(subset=['possible_symbols'])

```

Another example using the news from CommonCrawl database:

```
from NewsTrader.newsfeed.commoncrawldatabase import CCnewsDatabase, get_news_from_domain


cc_news_database = CCnewsDatabase(date_string='2020-10-01 12:00:00', only_allowed_domains=True, allowed_domains=['marketwatch.com'])
cc_news = get_news_from_domain(cc_news_database)


```
