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
The first one is to extract news data from open-source datasets, such as _CommonCrawl_ and _GDELT_.
These datasets aim to democratize the access to news and have gain attention from the investment community.
The first one is the GDELT Project, which claims to be the largest open-source news database (NewsTrader.newsfeed.gdeltdatabase).
Speically, there is a sub news database of the GDELT Project, called Global Entity Graph, from which news are already processed by Google (NewsTrader.newsfeed.gegdatabase).
The second one is the Common Crawl Project.
We focus on the CC news database of the Common Crawl Project, as we are only interested in news articles, but not general webpages.
We showcase how to dive into these datasets and extract useful text data for the purpose of use in building language
models.

If the news article data is not available, an alternative way is to scrape down news articles from websites in a polite and gentle way, via fast scraping methods such as
_scrapy_, _goose_ and _newsplease_. 
We try to build a simple scrapy framework to get historical or live news from major news websites in a gentle manner(NewsTrader.newsfeed.downloader).
Besides scraping down html pages, another major challenge is to parse html pages from various sources in a structured way.
Thanks to the comprehensive extractor module of the wonderfull package _newsplease_, we can extract useful news information such as news title, news main text and news publish date in a relatively efficient way.