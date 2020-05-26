from twisted.internet import defer

from os_scrapy_rq_crawler.crawler import Crawler as BaseCrawler

SUPPORTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
BASE_MODULE_PATH = __loader__.name


class Crawler(BaseCrawler):
    default_settings = {
        "ENGINE": BASE_MODULE_PATH + ".engine.Engine",
        "SCHEDULER": BASE_MODULE_PATH + ".scheduler.Scheduler",
        "DOWNLOADER": BASE_MODULE_PATH + ".downloader.Downloader",
    }

    @defer.inlineCallbacks
    def crawl(self, *args, **kwargs):
        from twisted.internet import reactor

        which = f"{reactor.__module__}.{reactor.__class__.__name__}"
        assert (
            which == SUPPORTED_REACTOR
        ), f"{which} is not supported, must use {SUPPORTED_REACTOR}"
        yield super(Crawler, self).crawl(*args, **kwargs)
