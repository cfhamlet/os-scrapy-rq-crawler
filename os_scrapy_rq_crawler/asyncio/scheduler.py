import asyncio
import logging
import random
from typing import Optional
from collections import OrderedDict

from twisted.internet import defer

from os_scrapy_rq_crawler.utils import MemoryRequestQueue, as_future

logger = logging.getLogger(__name__)


class Slot(object):
    __slots__ = ("slot_id", "scheduler", "qids", "task")

    def __init__(self, slot_id, scheudler):
        self.slot_id = slot_id
        self.scheduler = scheudler
        self.qids = None
        self.task = None

    def remove_qid(self, qid):
        if self.qids is None:
            return
        elif not isinstance(self.qids, set) and self.qids == qid:
            self.qids = None
        elif qid in self.qids:
            self.qids.remove(qid)
            if len(self.qids) == 1:
                self.qids = self.qids.pop()

    def add_qid(self, qid):
        if self.qids is None:
            self.qids = qid
        elif not isinstance(self.qids, set) and self.qids != qid:
            self.qids = set([self.qids, qid])
        else:
            self.qids.add(qid)

    def start(self):
        if self.task is None:
            self.task = asyncio.ensure_future(self.schedule())

    def _clear(self):
        self.qids = None
        self.task = None

    async def _schedule(self) -> Optional[float]:
        if not self.qids:
            return None

        qid = self.qids
        if isinstance(qid, set):
            qid = random.choice(list(qid))
        request = await self.scheduler.next_request(qid)

        if request is None:
            if isinstance(self.qids, set):
                if qid in self.qids:
                    self.qids.remove(qid)
                    if len(self.qids) == 1:
                        self.qids = self.qids.pop()
            else:
                self.qids = None
            return None

        d = self.scheduler.fetch(request)
        await as_future(d)

    async def schedule(self):
        while self.qids:
            try:
                wait = await self.scheduler.future_in_pool(self._schedule)
                if wait is not None:
                    await asyncio.sleep(wait)
            except asyncio.CancelledError:
                break
        self._clear()


class Scheduler(object):
    def __init__(self, crawler):
        self.crawler = crawler
        self.engine = crawler.engine
        self.slots = OrderedDict()
        self.mq = MemoryRequestQueue()
        self.schedule_task = None
        self.stopping = False

    def open(self, spider):
        self.spider = spider

    def stop(self):
        if self.stopping:
            return self.stopping
        self.stopping = defer.Deferred()
        return self.stop()

    def start(self):
        logger.debug("Start")
        if not self.schedule_task:
            self.schedule_task = asyncio.ensure_future(self._schedule())

    async def _schedule(self):
        pass

    async def next_request(self, qid=None):
        return self.mq.pop(qid)

    def future_in_pool(self, f, *args, **kwargs):
        return self.engine.future_in_pool(f, *args, **kwargs)

    def fetch(self, request):
        return self.engine.fetch(request, self.spider)

    @classmethod
    def from_crawler(cls, crawler) -> "Scheduler":
        return cls(crawler)

    def enqueue_request(self, request, head=False):
        self.mq.push(request, head)

    def has_pending_requests(self) -> bool:
        return len(self.mq) > 0

    def close(self, reason):
        logger.debug("Close")
        self.mq.close()
