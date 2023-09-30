"""
Get news dataset from CommonCrawl
"""
import logging
import os
from io import BytesIO
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4.dammit import EncodingDetector
from newsplease import NewsPlease
from tqdm import tqdm
from warcio.archiveiterator import ArchiveIterator


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CCnewsDatabase:

    _CC_BASE_URL = "https://data.commoncrawl.org/"

    def __init__(self, date_string, only_allowed_domains, allowed_domains, dir_path='.', remove_cache=True):
        """
        :param date_string: date input to target specific cc news blocks. e.g. "2020-01-01 12:00:00".
        :param allowed_url_patterns: a list containing domain patterns, such as ['www.finanzen.net']
        :param dir_path: directory path where the downloaded data should be stored.
        """
        self.date_string = date_string
        self.only_allowed_domains = only_allowed_domains
        self.allowed_domains = set(allowed_domains)
        self.dir_path = dir_path
        self.remove_cache = remove_cache

        """
        :param date_string: date input to target specific cc news blocks. e.g. "2020-01-01 12:00:00".
        :param allowed_url_patterns: a list containing domain patterns, such as ['www.finanzen.net']
        :param dir_path: directory path where the downloaded data should be stored.
        """
        self.date_string = date_string
        self.only_allowed_domains = only_allowed_domains
        self.allowed_domains = set(allowed_domains)
        self.dir_path = dir_path

    def query_urls(self):
        """
        Query CC news on AWS S3.
        :return: The list of filtered URLs.
        """
        date = pd.to_datetime(self.date_string)
        path_url = f'{self._CC_BASE_URL}crawl-data/CC-NEWS/{date.year}/{date.month:02}/warc.paths.gz'
        response = requests.get(path_url)
        df = pd.read_csv(BytesIO(response.content), compression='gzip')
        url_list = df[df.columns[0]].tolist()
        time_str = f'{date.year}{date.month:02}{date.day:02}{date.hour:02}'
        filtered_url_list = [f'{self._CC_BASE_URL}{url}' for url in url_list if time_str in url]
        return filtered_url_list


    def download_cc_warc_file(self, download_url):
        """
        Download cc news from AWS.
        :param download_url: query results returned by query_aws_s3.
        :return: the file path of the downloaded data.
        """
        file_name = download_url.split("/")[-1]
        file_path = os.path.join(self.dir_path, file_name)

        if os.path.exists(file_path):
            file_size = os.stat(file_path).st_size / 1e9
            if file_size > 1:
                logger.info(f"File {file_name} already downloaded")
                return file_path
            else:
                logger.info(f"File {file_name} is not completely downloaded")
                os.remove(file_path)

        logger.info(f"Downloading {download_url}")
        response = requests.get(download_url, stream=True)
        if response.status_code == 200:
            with open(file_path, "wb") as f:
                content_length = int(response.headers["Content-Length"])
                progress_bar = tqdm(unit="B", total=content_length)
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        progress_bar.update(len(chunk))
                        f.write(chunk)
        logger.info(f"Downloaded {download_url}")
        return file_path
    

    @staticmethod
    def get_html_from_record(record, decode_errors="replace"):
        """
        Decodes the HTML content from a record using the provided encoding. If the encoding cannot be determined from the record's HTTP headers, it defaults to 'utf-8'.

        Args:
            record (Record): The record containing the raw HTML content.
            decode_errors (str, optional): The error handling scheme to use when decoding the HTML content. Defaults to "replace".

        Returns:
            str: The decoded HTML content.
        """
        raw_stream = record.raw_stream.read()
        encoding = record.http_headers.get_header('Content-Type').split('=', 1)[-1].split(';', 1)[0] if record.http_headers.get_header('Content-Type') else None
        encoding = encoding or EncodingDetector.find_declared_encoding(raw_stream, is_html=True) or 'utf-8'
        try:
            html = raw_stream.decode(encoding, errors=decode_errors)
        except LookupError:
            html = raw_stream.decode('utf-8', errors=decode_errors)
        return html


    def get_html_from_warc(self, file_path):
        """
        Retrieves HTML content from a WARC file.

        Parameters:
            file_path (str): The path to the WARC file.

        Returns:
            pd.DataFrame: A DataFrame containing the extracted URLs and HTML content.
        """
        urls = []
        htmls = []
        with open(file_path, 'rb') as stream:
            for record in tqdm(ArchiveIterator(stream)):
                try:
                    if record.rec_type != 'response':
                        continue

                    url = record.rec_headers.get_header('WARC-Target-URI')
                    if self.only_allowed_domains:
                        domain = urlparse(url).netloc
                        if 'www.' in domain:
                            domain = domain[4:]
                        if domain not in self.allowed_domains:
                            continue

                    html = self.get_html_from_record(record)
                    htmls.append(html)
                    urls.append(url)
                except:
                    continue
        
        if self.remove_cache:
            os.remove(file_path)

        return pd.DataFrame({'url': urls, 'html': htmls})


    def parse_cc_warc_file(self, file_path):
        """
        Parse the downloaded cc warc file and extract the news data filtered by allowed url patterns.
        :param file_path: where warc files are stored.
        :return: a DataFrame containing the parse result, i.e., the desired news records
        """
        url_list = []
        date_publish_list = []
        date_download_list = []
        date_modify_list = []
        title_list = []
        text_list = []
        
        with open(file_path, 'rb') as stream:
            for record in ArchiveIterator(stream):
                if record.rec_type == 'response':
                    url = record.rec_headers.get_header('WARC-Target-URI')
                    if self.only_allowed_domains:
                        domain = urlparse(url).netloc
                        if 'www.' in domain:
                            domain = domain[4:]
                        if domain not in self.allowed_domains:
                            continue
                    article = NewsPlease.from_warc(record)
                    url_list.append(url)
                    date_publish_list.append(article.date_publish)
                    date_download_list.append(article.date_download)
                    date_modify_list.append(article.date_modify)
                    text_list.append(article.maintext)
                    title_list.append(article.title)

        if self.remove_cache:
            os.remove(file_path)

        return pd.DataFrame({'url': url_list,
                            'date_publish': date_publish_list,
                            'date_download': date_download_list,
                            'date_modify': date_modify_list,
                            'title': title_list,
                            'article_text': text_list})


def get_info_from_domain(cc_news_database, news_or_html = 'news'):
    """
    Retrives news data from specific domains.
    :return: a DataFrame containing the news data
    """
    query_result = cc_news_database.query_urls()
    if not query_result:
        return None
    result_df_list = []
    for result in query_result:
        file_path = cc_news_database.download_cc_warc_file(result)
        if news_or_html == 'news':
            func = cc_news_database.parse_cc_warc_file
        else:
            func = cc_news_database.get_html_from_warc
        df = func(file_path)
        result_df_list.append(df)

    final_df = pd.concat(result_df_list)
    return final_df
