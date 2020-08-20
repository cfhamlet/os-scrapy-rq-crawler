import asyncio
import inspect
import logging
import random
import time
from collections import OrderedDict
from typing import Optional

import async_timeout
from scrapy.http import Request, Response
from scrapy.utils.misc import create_instance, load_object

from os_scrapy_rq_crawler.utils import (
    QueueID,
    as_future,
    cancel_futures,
    class_fullname,
)

logger = logging.getLogger(__name__)

S_DOWNLOAD_DELAY = "download_delay"


class Slot(object):
    __slots__ = ("slot_id", "scheduler", "qids", "task")

    def __init__(self, scheduler, slot_id: str):
        self.slot_id = slot_id
        self.scheduler = scheduler
        self.qids = set()
        self.task = None

    def remove_qid(self, qid: QueueID):
        self.qids.discard(qid)
        if qid in self.scheduler.qids:
            self.scheduler.qids.pop(qid)

    def add_qid(self, qid: QueueID):
        self.qids.add(qid)
        if qid not in self.scheduler.qids:
            self.scheduler.qids[qid] = (self.slot_id, time.time())

    def start(self):
        if self.task is None:
            self.task = asyncio.ensure_future(self._schedule())

    def _clear(self):
        if not self.task:
            return
        for qid in list(self.qids):
            self.remove_qid(qid)
        self.task = None
        self.scheduler.slots.pop(self.slot_id, None)

    async def schedule(self) -> Optional[float]:
        if not self.qids:
            return

        qid = random.choice(list(self.qids))
        request = self.scheduler.next_request(qid)
        if inspect.isawaitable(request):
            request = await request

        if request is None:
            self.remove_qid(qid)
            return

        request.meta["download_slot"] = self.slot_id
        self.log(f"crawl {qid} {request}")

        def on_downloaded(response, request, spider):
            if self.slot_id.startswith("standby") and isinstance(response, Response):
                self.remove_qid(qid)
            elif isinstance(response, Request):
                if "rq.enqueue" not in response.meta:
                    response.meta["rq.enqueue"] = "lifo"
                response.meta["rq.qid"] = qid
            return response

        d = self.scheduler.fetch(request, on_downloaded)
        await as_future(d)
        if S_DOWNLOAD_DELAY in request.meta:
            return request.meta[S_DOWNLOAD_DELAY]
        return self.scheduler.download_delay()

    def next_loop(self) -> bool:
        return bool(self.qids)

    def log(self, msg: str, lvl=logging.DEBUG):
        logger.log(lvl, f"{self.slot_id} {msg}")

    async def _schedule(self):
        self.log("slot schedule start")
        while self.next_loop():
            try:
                delay = await self.scheduler.future_in_pool(self.schedule)
                if delay is not None:
                    try:
                        delay = float(delay)
                        if delay < 0:
                            raise ValueError("must >= 0")
                    except Exception as e:
                        self.log(f"invalid delay {repr(delay)} {e}", logging.WARNING)
                        delay = self.scheduler.download_delay()
                    self.log(f"download delay {delay}")
                    await asyncio.sleep(delay)
            except asyncio.CancelledError:
                self.log("slot schedule cancelled", logging.WARNING)
                break
            except Exception as e:
                self.log(f"unexpect error {e}", logging.ERROR)
                break
        self._clear()
        self.log("slot schedule stopped")


class Scheduler(object):
    def __init__(self, crawler, rq, max_slots, standby_slots, stats=None):
        self.crawler = crawler
        self.settings = crawler.settings
        self.rq = rq
        self.stats = stats
        self.engine = crawler.engine
        self.stopping = False
        self.slots = OrderedDict()
        self.dispatch_queue: Optional[asyncio.Queue] = None
        self.tasks = []
        self.spider = None
        self.ip_concurrency = self.settings.getint("CONCURRENT_REQUESTS_PER_IP")
        self._download_delay = self.settings.getfloat("DOWNLOAD_DELAY")
        self.max_slots = max_slots
        self.standby_slots = standby_slots
        self.qids = {}

    def download_delay(self) -> float:
        if hasattr(self.spider, S_DOWNLOAD_DELAY):
            return spider.download_delay
        return self._download_delay

    def should_stop(self) -> bool:
        return self.stopping and self.engine.spider_is_idle(self.spider)

    def standby_slot_id(self, qid: QueueID):
        slot_id = "standby"
        if self.standby_slots == 1:
            pass
        elif self.standby_slots > 1:
            slot_id = f"standby_{hash(qid.host) % self.standby_slots}"
        else:
            slot_id = str(qid)
        return slot_id

    async def _dispatch_slot(self, qid: QueueID):
        if not qid or qid in self.qids:
            return
        from twisted.internet import reactor

        slot_id = str(qid)
        if self.ip_concurrency:
            d = reactor.resolver.getHostByName(qid.host)
            try:
                ip = await as_future(d)
                slot_id = ":".join((ip, qid.port, qid.scheme))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                slot_id = self.standby_slot_id(qid)
                logger.warn(f"get slot fail {qid} {e} use {slot_id}")
        if slot_id not in self.slots:
            logger.debug(f"new slot {slot_id} {qid}")
            slot = Slot(self, slot_id)
            slot.start()
            self.slots[slot_id] = slot
        self.slots[slot_id].add_qid(qid)

    async def _dispatch(self, did):
        logger.debug(f"dispatch-{did} start")
        while not self.should_stop():
            try:
                if self.stopping:
                    with async_timeout.timeout(0.3):
                        qid = await self.dispatch_queue.get()
                else:
                    qid = await self.dispatch_queue.get()
            except (asyncio.CancelledError, asyncio.TimeoutError):
                continue
            try:
                await self._dispatch_slot(qid)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"dispatch error {qid} {e}")
            finally:
                self.dispatch_queue.task_done()
        logger.debug(f"dispatch-{did} stopped")

    def open(self, spider):
        self.spider = spider

    def stop(self):
        if self.stopping:
            return self.stopping
        self.stopping = cancel_futures(self.tasks)
        return self.stopping

    async def update_slots(self):
        s = time.time()
        k = self.max_slots - len(self.slots)
        if k > 0:
            qids = self.rq.qids(k=k)
            if qids:
                if inspect.isawaitable(qids):
                    qids = await qids
                puts = [
                    self.dispatch_queue.put(qid) for qid in qids if qid not in self.qids
                ]
                if puts:
                    await asyncio.wait(puts, return_when=asyncio.ALL_COMPLETED)
        cost = time.time() - s
        if cost < 1:
            await asyncio.sleep(1 - cost)

    def start(self):
        logger.debug("Start")
        self.tasks.append(asyncio.ensure_future(self._schedule()))
        dsp = self.crawler.settings.getint(
            "SCHEDULE_DISPATCH_TASKS", self.standby_slots
        )
        dsp = int(dsp) if dsp > 1 else 1
        self.dispatch_queue = asyncio.Queue(dsp)
        for i in range(dsp):
            self.tasks.append(asyncio.ensure_future(self._dispatch(i)))

    async def _schedule(self):
        while not self.should_stop():
            try:
                await self.update_slots()
            except asyncio.CancelledError:
                logger.debug("cancel schedule task")
            except Exception as e:
                logger.error(f"schedule {e}")
        logger.debug(f"schedule task stopped")

    def next_request(self, qid=None):
        return self.rq.pop(qid)

    def future_in_pool(self, f, *args, **kwargs):
        return self.engine.future_in_pool(f, *args, **kwargs)

    def fetch(self, request, on_downloaded=None):
        return self.engine.fetch(request, self.spider, on_downloaded)

    @classmethod
    def from_crawler(cls, crawler) -> "Scheduler":
        settings = crawler.settings
        rq_cls = load_object(
            settings.get(
                "SCHEDULER_REQUEST_QUEUE", "os_scrapy_rq_crawler.MemoryRequestQueue",
            )
        )
        rq = create_instance(rq_cls, settings, crawler)
        logger.debug(f"Using request queue: {class_fullname(rq_cls)}")
        concurrency = settings.getint("CONCURRENT_REQUESTS", 16)
        delay = settings.getint("DOWNLOAD_DELAY")
        max_slots = settings.getint(
            "SCHEDULER_MAX_SLOTS", concurrency * (delay if delay > 0 else 3)
        )
        assert max_slots > 1, f"SCHEDULER_MAX_SLOTS({max_slots}) must > 1"
        standby_slots = settings.getint("SCHEDULER_STANDBY_SLOTS", int(concurrency / 4))
        logger.debug(
            f"max_slots:{max_slots} standby_slots:{standby_slots} concurrency:{concurrency}"
        )
        return cls(crawler, rq, max_slots, standby_slots, crawler.stats)

    def enqueue_request(self, request):
        self.rq.push(request)
        if self.stats:
            self.stats.inc_value("scheduler/enqueued", spider=self.spider)

    def has_pending_requests(self) -> bool:
        return len(self.rq) > 0

    def close(self, reason):
        self.rq.close()
        logger.debug("Closed")
