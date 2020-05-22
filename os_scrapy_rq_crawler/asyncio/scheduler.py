import asyncio
import inspect
import logging
import random
from collections import OrderedDict
from typing import Optional

from expiringdict import ExpiringDict
from scrapy.utils.misc import create_instance, load_object

from os_scrapy_rq_crawler.utils import as_future, cancel_futures, class_fullname

logger = logging.getLogger(__name__)


class Slot(object):
    __slots__ = ("slot_id", "scheduler", "qids", "task")

    def __init__(self, scheduler, slot_id):
        self.slot_id = slot_id
        self.scheduler = scheduler
        self.qids = None
        self.task = None

    def remove_qid(self, qid):
        if self.qids is None:
            return
        elif not isinstance(self.qids, set):
            if self.qids == qid:
                self.qids = None
        elif qid in self.qids:
            self.qids.remove(qid)
            if len(self.qids) == 1:
                self.qids = self.qids.pop()

    def add_qid(self, qid):
        if self.qids is None:
            self.qids = qid
        elif not isinstance(self.qids, set):
            if self.qids != qid:
                self.qids = set([self.qids, qid])
        else:
            self.qids.add(qid)

    def start(self):
        if self.task is None:
            self.task = asyncio.ensure_future(self._schedule())

    def _clear(self):
        if self.task:
            self.qids = None
            self.task = None
            self.scheduler.slots.pop(self.slot_id, None)

    async def schedule(self) -> Optional[float]:
        if not self.qids:
            return

        qid = self.qids
        if isinstance(qid, set):
            qid = random.choice(list(qid))
        request = self.scheduler.next_request(qid)
        if inspect.isawaitable(request):
            request = await request

        if request is None:
            self.remove_qid(qid)
            return
        request.meta["download_slot"] = self.slot_id
        logger.debug(f"crawl {self.slot_id} {qid} {request}")
        d = self.scheduler.fetch(request)
        await as_future(d)
        return 10

    def next_loop(self) -> bool:
        return bool(self.qids)

    def log(self, msg, lvl=logging.DEBUG):
        logger.log(lvl, f"{self.slot_id} {msg}")

    async def _schedule(self):
        self.log("slot schedule start")
        while self.next_loop():
            try:
                wait = await self.scheduler.future_in_pool(self.schedule)
                if wait is not None:
                    self.log(f"wait {wait}")
                    await asyncio.sleep(wait)
            except asyncio.CancelledError:
                self.log("slot schedule cancelled", logging.WARNING)
                break
            except Exception as e:
                self.log(f"unexpect error {e}", logging.ERROR)
                break
        self.log("slot schedule stopped")
        self._clear()


class Scheduler(object):
    def __init__(self, crawler, rq, stats=None):
        self.crawler = crawler
        self.settings = crawler.settings
        self.rq = rq
        self.stats = stats
        self.engine = crawler.engine
        self.stopping = False
        self.slots = OrderedDict()
        self.dispatch_queue = None
        self.start_requests_event = None
        self.tasks = []
        self.ip_concurrency = self.settings.getint("CONCURRENT_REQUESTS_PER_IP")
        standby_slots = self.settings.getint("STANDBY_SLOTS")
        if not standby_slots:
            standby_slots = self.settings.getint("CONCURRENT_REQUESTS", 16) / 4
        self.standby_slots = int(standby_slots) if int(standby_slots) > 0 else 1
        self.qids_cache = ExpiringDict(max_len=10000, max_age_seconds=60)

    def should_stop(self) -> bool:
        return self.stopping and self.engine.spider_is_idle(self.spider)

    async def _dispatch_slot(self, qid):
        if not qid or qid in self.qids_cache:
            return
        from twisted.internet import reactor

        slot_id = qid.host
        if self.ip_concurrency:
            d = reactor.resolver.getHostByName(qid.host)
            try:
                slot_id = await as_future(d)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                slot_id = f"standby_{hash(qid.host) % self.standby_slots}"
                logger.warn(f"get slot fail {qid} {e} use {slot_id}")
        self.qids_cache[qid] = 0
        if slot_id not in self.slots:
            logger.debug(f"new slot {slot_id} {qid.host}")
            slot = Slot(self, slot_id)
            slot.start()
            self.slots[slot_id] = slot
        self.slots[slot_id].add_qid(qid)

    async def _dispatch(self):

        while not self.should_stop():
            try:
                qid = await self.dispatch_queue.get()
            except asyncio.CancelledError:
                continue
            try:
                await self._dispatch_slot(qid)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"dispatch error {qid} {e}")
            finally:
                self.dispatch_queue.task_done()

    def open(self, spider):
        self.spider = spider

    def stop(self):
        if self.stopping:
            return self.stopping
        self.stopping = cancel_futures(self.tasks)
        return self.stopping

    async def update_slots(self):
        if self.start_requests_event:
            await self.start_requests_event.wait()
            self.start_requests_event = None
        qids = self.rq.qids()
        if qids:
            if inspect.isawaitable(qids):
                qids = await qids
            for qid in qids:
                if qid not in self.qids_cache:
                    await self.dispatch_queue.put(qid)
        await asyncio.sleep(1)

    def start(self):
        logger.debug("Start")
        if self.engine.slot.start_requests:
            self.start_requests_event = asyncio.Event()
        self.tasks.append(asyncio.ensure_future(self._schedule()))
        num = self.crawler.settings.getint(
            "SCHEDULE_DISPATCH_TASKS", self.standby_slots
        )
        self.dispatch_queue = asyncio.Queue(num)
        for _ in range(num):
            self.tasks.append(asyncio.ensure_future(self._dispatch()))

    async def _schedule(self):
        while not self.should_stop():
            try:
                await self.update_slots()
            except asyncio.CancelledError:
                logger.debug("cancel schedule task")
                continue
            except Exception as e:
                logger.error(f"schedule {e}")
        logger.debug(f"schedule task stopped")

    def next_request(self, qid=None):
        return self.rq.pop()

    def future_in_pool(self, f, *args, **kwargs):
        return self.engine.future_in_pool(f, *args, **kwargs)

    def fetch(self, request):
        return self.engine.fetch(request, self.spider)

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
        return cls(crawler, rq, crawler.stats)

    def enqueue_request(self, request, head=False):
        if self.start_requests_event and not self.start_requests_event.is_set():
            self.start_requests_event.set()
        self.rq.push(request, head)
        if self.stats:
            self.stats.inc_value("scheduler/enqueued", spider=self.spider)

    def has_pending_requests(self) -> bool:
        return len(self.rq) > 0

    def close(self, reason):
        self.rq.close()
        logger.debug("Closed")
