import asyncio
import logging
from time import time

from scrapy import signals
from scrapy.core.engine import ExecutionEngine
from scrapy.utils.defer import maybeDeferred_coro
from scrapy.utils.log import failure_to_exc_info
from twisted.internet import defer

from os_scrapy_rq_crawler.utils import Pool

logger = logging.getLogger(__name__)


class NextCall(object):
    def schedule(self):
        pass


class Slot(object):
    def __init__(
        self, engine: "Engine", spider, scheduler, start_requests, close_if_idle: bool
    ):
        self.engine = engine
        self.spider = spider
        self.closing = False
        self.close_wait = None
        self.inprogress = set()
        self.close_if_idle = close_if_idle
        self.scheduler = scheduler
        self.nextcall = NextCall()
        self.start_requests = iter(start_requests)
        self.tasks = []

    def add_request(self, request):
        self.inprogress.add(request)

    def remove_request(self, request):
        self.inprogress.remove(request)
        self._maybe_fire_closing()

    def start(self):
        for f in (self._load_start_requests, self._close_idle):
            self.tasks.append(asyncio.ensure_future(f()))

    async def _close_idle(self):
        self.engine._spider_idle(self.spider)
        while self.close_if_idle and self.engine.slot:
            if self.engine.spider_is_idle(self.spider):
                self.engine._spider_idle(self.spider)
                break
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                logger.debug("close idle task cancelled")
                break

    async def _load_start_requests(self):
        if self.start_requests:
            for request in self.start_requests:
                try:
                    await self.engine.future_in_pool(
                        self.engine.crawl, request, self.spider
                    )
                except asyncio.CancelledError:
                    logger.warn("load start requests task cancelled")
                    break
        self.start_requests = None

    def close(self):
        if self.closing:
            return self.closing
        self.close_wait = defer.Deferred()
        dlist = [self.close_wait]
        for task in self.tasks:
            if task and not task.done():
                task.cancel()

            async def wait(coro):
                await coro

            dlist.append(maybeDeferred_coro(wait, task))

        dlist.append(self.scheduler.stop())
        self.closing = defer.DeferredList(dlist)
        self._maybe_fire_closing()
        return self.closing

    def _maybe_fire_closing(self):
        if self.closing and not self.inprogress:
            self.close_wait.callback(None)


class Engine(ExecutionEngine):
    def __init__(self, crawler, spider_closed_callback):
        super(Engine, self).__init__(crawler, spider_closed_callback)
        self._pool = Pool(self.crawler.settings.getint("CONCURRENT_REQUESTS", 16))

    @defer.inlineCallbacks
    def open_spider(self, spider, start_requests=(), close_if_idle=True):
        logger.info("Spider opened", extra={"spider": spider})
        self.spider = spider
        scheduler = self.scheduler_cls.from_crawler(self.crawler)
        start_requests = yield self.scraper.spidermw.process_start_requests(
            start_requests, spider
        )
        self.slot = Slot(self, spider, scheduler, start_requests, close_if_idle)
        yield scheduler.open(spider)
        yield self.scraper.open_spider(spider)
        self.crawler.stats.open_spider(spider)
        yield self.signals.send_catch_log_deferred(signals.spider_opened, spider=spider)

    @defer.inlineCallbacks
    def start(self):
        """Start the execution engine"""
        if self.running:
            raise RuntimeError("Engine already running")
        self.start_time = time()
        yield self.signals.send_catch_log_deferred(signal=signals.engine_started)
        self.running = True
        yield self.slot.scheduler.start()
        yield self.slot.start()
        self.unpause()
        self._closewait = defer.Deferred()
        yield self._closewait

    def future_in_pool(self, f, *args, **kwargs):
        return self._pool.maybeFuture(f, *args, **kwargs)

    def pause(self):
        self._pool.pause()

    def unpause(self):
        self._pool.unpause()

    def crawl(self, request, spider):
        if spider not in self.open_spiders:
            raise RuntimeError(
                "Spider %r not opened when crawling: %s" % (spider.name, request)
            )
        self.schedule(request, spider)

    def fetch(self, request, spider):
        slot = self.slot
        d = self._download(request, spider)
        d.addBoth(self._handle_downloader_output, request, spider)
        d.addErrback(
            lambda f: logger.info(
                "Error while handling downloader output",
                exc_info=failure_to_exc_info(f),
                extra={"spider": spider},
            )
        )
        d.addBoth(lambda _: slot.remove_request(request))
        d.addErrback(
            lambda f: logger.info(
                "Error while removing request from slot",
                exc_info=failure_to_exc_info(f),
                extra={"spider": spider},
            )
        )
        return d
