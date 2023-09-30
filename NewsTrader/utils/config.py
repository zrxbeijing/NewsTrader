"""
Scrapy config
"""

BROAD_CRAWL_SETTINGS = {
    'DOWNLOAD_DELAY': 2,  # for the same domain
    'SCHEDULER_PRIORITY_QUEUE': 'scrapy.pqueues.DownloaderAwarePriorityQueue',
    'ITEM_PIPELINES': {
        'NewsTrader.utils.pipelines.JsonWriterPipeline': 300
    },
    'CONCURRENT_REQUESTS': 500,
    'ROBOTSTXT_OBEY': True,
    'REACTOR_THREADPOOL_MAXSIZE': 100,
    'LOG_LEVEL': 'INFO',
    'COOKIES_ENABLED': False,
    'RETRY_ENABLED': False,
    'DOWNLOAD_TIMEOUT ': 5,
    'REDIRECT_ENABLED': False,
    'AJAXCRAWL_ENABLED': True,
    'DEPTH_PRIORITY': 1,
    'SCHEDULER_DISK_QUEUE': 'scrapy.squeues.PickleFifoDiskQueue',
    'SCHEDULER_MEMORY_QUEUE': 'scrapy.squeues.FifoMemoryQueue',
    'DEFAULT_REQUEST_HEADERS': {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/67.0.3396.99 Safari/537.36'}
}
