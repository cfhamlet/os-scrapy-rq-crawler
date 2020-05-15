import asyncio
import random
import time
import warnings

from scrapy.utils.log import logger

from os_scrapy_rq_crawler.utils import as_deferred, queues_from_rq, request_from_rq


class DeferredAsyncRQ(object):
    def __init__(self, rq_api, rq_timeout=10, max_concurrent=16, crawler=None):
        self.rq_api = rq_api
        self.rq_timeout = rq_timeout
        self.max_concurrent = max_concurrent
        self.lock = asyncio.Lock()
        self.pending_queues = dict()
        self.update_queues_time = 0
        self.crawler = crawler
        self.backpressure = 0
        self.engine = crawler.engine

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        rq_api = settings.get("RQ_API", "http://localhost:6789/api/")
        rq_timeout = settings.getint("RQ_TIMEOUT", 10)
        max_concurrent = settings.getint("CONCURRENT_REQUESTS", 16)
        if max_concurrent > 500:
            warnings.warn(
                "CONCURRENT_REQUESTS > 500 is meaningless because of rq api limit, set to 500 automaticly"
            )
            max_concurrent = 500

        return cls(rq_api, rq_timeout, max_concurrent, crawler)

    def update_interval(self):
        return time.time() - self.update_queues_time

    async def update_queues(self):
        l = len(self.pending_queues)
        if l > 0 and self.update_interval() < 1:
            return
        if self.lock.locked() and l > 0:
            return
        with await self.lock:
            if self.update_interval() >= 1:
                await self.update_queues_from_rq()

    async def update_queues_from_rq(self):
        try:
            await self._update_queues_from_rq()
        except Exception as e:
            logger.warn("Can not update queues %s %s" % (self.rq_api, str(e)))
            self._reset()
        self.update_queues_time = time.time()

    async def _update_queues_from_rq(self):
        status, ret, api_url = await queues_from_rq(
            self.rq_api, self.max_concurrent * 2, timeout=self.rq_timeout
        )
        if status != 200 or "queues" not in ret:
            logger.warn("Can not get queues from %s %d %s" % (api_url, status, ret))
            self._reset()
            return
        oq = len(self.pending_queues)
        ot = self.backpressure
        self.pending_queues = dict([(q["qid"], q["qsize"]) for q in ret["queues"]])
        nq = len(self.pending_queues)
        nt = sum(self.pending_queues.values())
        self.backpressure = nt
        logger.debug(
            "Update queues from %s oq=%d ot=%d nq=%d nt=%d" % (api_url, oq, ot, nq, nt)
        )
        if nt - ot > 0:
            self.engine.slot.nextcall.schedule()

    def _reset(self):
        self.pending_queues.clear()
        self.backpressure = 0

    async def next_queue(self):
        while True:
            await self.update_queues()
            if not self.pending_queues:
                break
            qid = random.sample(self.pending_queues.keys(), 1)[0]
            return qid

    async def get_request_from_rq(self, qid):
        request = n = None
        try:
            status, request, api_url = await request_from_rq(
                self.rq_api, qid, timeout=self.rq_timeout
            )
            if status == 200:
                logger.debug("Get request from %s %s" % (api_url, str(request)))
                n = 1
            else:
                logger.warn(
                    "Can not get request from %s %d %s" % (api_url, status, request)
                )
                request = None
        except Exception as e:
            logger.error("Can not get request %s %s" % (qid, str(e)))
        self._adjust_queues(qid, n)
        return request

    def _adjust_queues(self, qid, n=None):
        if qid in self.pending_queues:
            if n is None:
                self.pending_queues.pop(qid)
            else:
                c = self.pending_queues[qid]
                c -= n
                if c <= 0:
                    self.pending_queues.pop(qid)
                else:
                    self.pending_queues[qid] = c
        if len(self.pending_queues) <= 0:
            self._reset()

    async def request_from_rq(self):
        while True:
            qid = await self.next_queue()
            if not qid:
                return None
            request = await self.get_request_from_rq(qid)
            if request:
                return request

    async def next_request(self):
        request = None
        try:
            request = await self.request_from_rq()
        except Exception as e:
            logger.error("Error while getting new request from rq", e)
            self._reset()
        return request

    def pop(self):
        if self.backpressure > 0 or (
            self.backpressure == 0 and self.update_interval() >= 1
        ):
            d = as_deferred(self.next_request())
            self.backpressure -= 1
            return d
