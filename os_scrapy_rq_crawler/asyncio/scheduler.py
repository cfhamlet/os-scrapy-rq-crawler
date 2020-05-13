from os_scrapy_rq_crawler.asyncio.rq import DeferredAsyncRQ
from scrapy.core.scheduler import Scheduler as ScrapyScheduler
from twisted.internet import defer


class Scheduler(object):
    def __init__(self, rq, stats=None, crawler=None):
        self.rq = rq
        self.stats = stats
        self.crawler = crawler
        self.scheduler = ScrapyScheduler.from_crawler(crawler)
        self.spider = None

    @classmethod
    def from_crawler(cls, crawler):
        rq = DeferredAsyncRQ.from_crawler(crawler)
        return cls(rq, crawler.stats, crawler)

    def next_request(self):
        d = None
        request = self.scheduler.next_request()
        if request:
            d = defer.Deferred()
            d.callback(request)
        else:
            d = self.rq.pop()
            if d:

                def _stats(request):
                    if request:
                        self.stats.inc_value(
                            "scheduler/dequeued/rq", spider=self.spider
                        )
                    return request

                d.addCallback(_stats)
        return d

    def enqueue_request(self, request):
        return self.scheduler.enqueue_request(request)

    def open(self, spider):
        self.spider = spider
        return self.scheduler.open(spider)

    def close(self, reason):
        return self.scheduler.close(reason)

    def has_pending_requests(self):
        return True
