import asyncio
import logging
from time import time

from scrapy import signals
from scrapy.core.engine import ExecutionEngine
from scrapy.utils.log import failure_to_exc_info
from scrapy.utils.reactor import CallLaterOnce
from twisted.internet import defer

from os_scrapy_rq_crawler.utils import Pool, cancel_futures

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
        self.start_requests = iter(start_requests) if start_requests else ()
        self.tasks = []
        self.start_requests_deferred = defer.Deferred()

    def _maybe_first_request(self):
        if self.start_requests_deferred:
            self.start_requests_deferred.callback(None)
            self.start_requests_deferred = None

    def add_request(self, request):
        self.inprogress.add(request)

    def remove_request(self, request):
        self.inprogress.remove(request)
        self._maybe_fire_closing()

    def start(self):
        for f in [
            self._load_start_requests,
        ]:
            self.tasks.append(asyncio.ensure_future(f()))
        return self.start_requests_deferred

    async def _load_start_requests(self):
        count = 0
        for request in self.start_requests:
            count += 1
            try:
                await self.engine.future_in_pool(
                    self.engine.crawl, request, self.spider
                )
                self._maybe_first_request()
                logger.debug(f"load start request {count} {request}")
            except asyncio.CancelledError:
                logger.warn("load start requests task cancelled")
                break
            except Exception as e:
                logger.error(f"load start request fail {request} {e}")
        self._maybe_first_request()
        logger.debug(f"load start requests {count} stopped")
        self.start_requests = None
        CallLaterOnce(self._maybe_fire_closing).schedule()

    def close(self):
        if self.closing:
            return self.closing
        self.close_wait = defer.Deferred()
        dlist = [self.close_wait]
        dlist.append(cancel_futures(self.tasks))
        dlist.append(self.scheduler.stop())
        self.closing = defer.DeferredList(dlist)
        self._maybe_fire_closing()
        return self.closing

    def spider_is_idle(self) -> bool:
        if self.close_if_idle or self.closing:
            return not self.inprogress and self.engine.spider_is_idle(self.spider)
        return False

    def _maybe_fire_closing(self):
        if self.spider_is_idle():
            if self.close_if_idle:
                self.engine._spider_idle(self.spider)
            if self.close_wait:
                self.close_wait.callback(None)
                self.close_wait = None


class Engine(ExecutionEngine):
    def __init__(self, crawler, spider_closed_callback):
        super(Engine, self).__init__(crawler, spider_closed_callback)
        self._pool = Pool(self.crawler.settings.getint("CONCURRENT_REQUESTS", 16))

    @defer.inlineCallbacks
    def open_spider(self, spider, start_requests=(), close_if_idle=True):
        logger.info("Spider opened", extra={"spider": spider})
        self.spider = spider
        scheduler = self.scheduler_cls.from_crawler(self.crawler)
        if start_requests:
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
        self.unpause()
        yield self.slot.start()
        yield self.slot.scheduler.start()
        self._closewait = defer.Deferred()
        yield self._closewait

    def future_in_pool(self, f, *args, **kwargs):
        return self._pool.maybeFuture(f, *args, **kwargs)

    def pause(self):
        self._pool.pause()

    def unpause(self):
        self._pool.unpause()

    def fetch(self, request, spider, on_downloaded=None):
        slot = self.slot
        d = self._download(request, spider)
        if on_downloaded:
            d.addBoth(on_downloaded, request, spider)
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
