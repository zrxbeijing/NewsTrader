import subprocess
import requests
import pandas as pd
import os
import logging
from warcio.archiveiterator import ArchiveIterator
from newsplease import NewsPlease
from tqdm import tqdm
from urllib.parse import urlparse


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CCnewsDatabase:
    """
    Extract news from common crawl news database.
    Common crawl is a open-source project where you can extract massive amount of news or webpages for free.
    This class queries specific data block based on timestamp and domains.
    Note that this class is inspired by the project https://github.com/fhamborg/news-please

    Example:
    First, we construct a CCnewsDatabase class.
    If we want to extract CC news at 2020-01-01 12:00:00, we specify the date_string as "2020-01-01 12:00:00".
    For most of the time, we are only interested in news from specific websites.
    Accordingly, we provide a list of allowed url patterns, e.g., ["tagesschau.de", "computerbase.de"] as follows:
    cc_news_database = CCnewsDatabase(date_string="2020-01-01 12:00:00", allowed_url_patterns=["tagesschau.de", "computerbase.de"])
    Then we can directly get target news from CC news Database on AWS: cc_news = cc_news_database.get_news_from_domain().
    A query will be sent to aws and if there is a matched result (CC-NEWS data package) it will be returned and the corresponding extracting will be initiated.

    """

    __CC_BASE_URL = "https://commoncrawl.s3.amazonaws.com/"
    __AWS_S3_QUERY_BASE = "aws s3 ls --recursive s3://commoncrawl/crawl-data/CC-NEWS/{}/CC-NEWS-{} --no-sign-request "

    def __init__(
        self, date_string, only_allowed_domains=True, allowed_domains=None, dir_path="."
    ):
        """
        :param date_string: date input to target specific cc news blocks. e.g. "2020-01-01 12:00:00".
        :param allowed_url_patterns: a list containing domain patterns, such as ['www.finanzen.net']
        :param dir_path: directory path where the downloaded data should be stored.
        """
        self.date_string = date_string
        self.only_allowed_domains = only_allowed_domains
        self.allowed_domains = set(allowed_domains)
        self.dir_path = dir_path

    def query_aws_s3(self):
        """
        Query cc news on aws s3.
        :param date_string: e.g. '2021-03-01 01:00:00', it is better to be clear on hour.
        :return: the query result returned by aws s3.
        """
        date = pd.to_datetime(self.date_string)
        first_param = date.strftime("%Y/%m")
        second_param = date.strftime("%Y%m%d%H")
        aws_s3_query = self.__AWS_S3_QUERY_BASE.format(first_param, second_param)
        try:
            output = subprocess.check_output(aws_s3_query, shell=True).decode("utf-8")
            query_results = output.strip().split("\n")
            query_results = [
                self.__CC_BASE_URL + result.split(" ")[-1] for result in query_results
            ]
            return query_results
        except subprocess.CalledProcessError:
            return None

    def download_cc_warc_file(self, download_url):
        """
        Download cc news from aws.
        :param download_url: query results returned by query_aws_s3.
        :return: the file path of the downloaded data.
        """
        file_name = download_url.split("/")[-1]
        file_path = os.path.join(self.dir_path, file_name)
        if os.path.exists(file_path):
            if os.stat(file_path).st_size / 1e9 > 1:
                logger.info("file {} already downloaded".format(file_name))
                return file_path
            else:
                logger.info("file {} is not completed downloaded".format(file_name))
                os.remove(file_path)
        logger.info("Downloading {}".format(download_url))
        response = requests.get(download_url, stream=True)
        if response.status_code == 200:
            with open(file_path, "wb") as f:
                pbar = tqdm(unit="B", total=int(response.headers["Content-Length"]))
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        pbar.update(len(chunk))
                        f.write(chunk)
        logger.info("Downloaded {}".format(download_url))
        return file_path

    def parse_cc_warc_file(self, file_path):
        """
        Parse the downloaded cc warc file and extract the news data filtered by allowed url patterns.
        :param file_path: where warc files are stored.
        :return: a DataFrame containing the parse result, i.e., the desired news records
        """

        def _get_domain_from_url(url):
            """
            Get domain from url.
            """
            domain = urlparse(url).netloc
            # remove "www."
            if "www." in domain:
                domain = domain[4:]
            return domain

        def _parse_record(record):
            """
            Parse warc record.
            """
            article = NewsPlease.from_warc(record)
            entry = [
                article.date_publish,
                article.date_download,
                article.date_modify,
                article.maintext,
                article.title,
            ]
            return entry

        # prepare an empty dataframe to store result
        result = pd.DataFrame(
            columns=[
                "url",
                "date_publish",
                "date_download",
                "date_modify",
                "title",
                "article_text",
            ]
        )

        with open(file_path, "rb") as stream:
            # iterate through warc records
            for record in tqdm(ArchiveIterator(stream)):
                # keep only records with type "response"
                if record.rec_type == "response":
                    url = record.rec_headers.get_header("WARC-Target-URI")
                    domain = _get_domain_from_url(url)
                    # whether allow only some specified domains or any domains
                    if not self.only_allowed_domains or domain in self.allowed_domains:
                        entry = _parse_record(record)
                        row = pd.Series([url] + entry, index=result.columns)
                        result = result.append(row, ignore_index=True)

        # remove warc file
        os.remove(file_path)
        return result


def get_info_from_domain(ccnews_database, news_or_html="news"):
    """
    Main entry point to get the desired news from specific domains.
    :return: a DataFrame containing the news data.
    """
    query_result = ccnews_database.query_aws_s3()
    if not query_result:
        print("There is no news for {}".format(ccnews_database.date_string))
        return
    result_df_list = []
    for result in query_result:
        file_path = ccnews_database.download_cc_warc_file(result)
        if news_or_html == "news":
            func = ccnews_database.parse_cc_warc_file
        else:
            func = ccnews_database.get_html_from_warc
        df = func(file_path)
        result_df_list.append(df)

    final_df = pd.concat(result_df_list)
    return final_df
