from scrapy.crawler import CrawlerProcess
from billiard import Process

crawler_settings = {
  "BOT_NAME": "supplier_scrapper",
  "HEADLESS": "True",
  "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
  "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
  "FEED_EXPORT_ENCODING":  "utf-8",
  'PLAYWRIGHT_BROWSER_TYPE': 'chromium',
  "HTTPERROR_ALLOWED_CODES" : [500, 502, 503, 504, 522, 524, 408, 400, 404, 401],
  "RETRY_HTTP_CODES" : [500, 502, 503, 504, 522, 524, 408, 400, 404, 401],
  "RETRY_TIMES": 3,
  "DOWNLOAD_HANDLERS": {
      "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
      "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
  },
  "ITEM_PIPELINES": {
      "app.scrapper.pipelines.ScrapperPipeline": 600,
  }
}

class UrlCrawlerScript(Process):
    def __init__(self, spider, kwargs):
        Process.__init__(self)
        self.crawler_process = CrawlerProcess(crawler_settings)
        self.spider = spider
        self.kwargs = kwargs

    def run(self):
        self.crawler_process.crawl(self.spider, **self.kwargs)
        self.crawler_process.start()
        self.crawler_process.join()

def run_spider(spider, kwargs):
    try:
        crawler = UrlCrawlerScript(spider, kwargs)
        crawler.start()
        crawler.join()
    except Exception as err:
        print(err.message)