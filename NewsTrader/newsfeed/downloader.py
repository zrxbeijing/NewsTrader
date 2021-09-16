import scrapy
import pandas as pd
from scrapy.crawler import CrawlerProcess
from newsplease import NewsPlease, NewscrawlerItem
from ..utils.config import BROAD_CRAWL_SETTINGS


class NewsSpider(scrapy.Spider):
    """
    Spider for broad crawling
    """

    name = "NewsSpider"

    def parse(self, response):
        self.logger.info("Got successful response from {}".format(response.url))
        html = response.text
        article = NewsPlease.from_html(html)
        item = NewscrawlerItem()
        item["url"] = response.url
        item["article_title"] = article.title
        item["article_text"] = article.maintext
        if article.date_publish:
            item["article_publish_date"] = str(article.date_publish)
        else:
            item["article_publish_date"] = article.date_publish

        yield item


def start_crawl(start_urls):
    process = CrawlerProcess(settings=BROAD_CRAWL_SETTINGS)
    process.crawl(NewsSpider, start_urls=start_urls)
    process.start()


def get_articles(urls):
    """
    Given geg record_df, get the original articles, by utilizing the scrapy fromework
    """
    start_crawl(urls)
    news_df = pd.read_json("items.jl", lines=True)
    news_df = news_df.drop_duplicates()
    url_df = pd.DataFrame({"url": urls})
    result_df = url_df.merge(news_df, how="left", on="url")
    return result_df
