import logging

from os_scrapy_rq_crawler.utils import HTTPRequestQueue, MemoryRequestQueue

logger = logging.getLogger(__name__)


class AsyncRequestQueue(object):
    def __init__(self, crawler, mq, rq):
        self.crawler = crawler
        self.mq = mq
        self.rq = rq

    async def qids(self, k=16):
        qids = [] if len(self.mq) <= 0 else self.mq.qids(k)
        if self.closing():
            return qids
        r = k - len(qids)
        if r <= 0:
            return qids
        rqids = []
        try:
            rqids = await self.rq.qids(r)
        except Exception as e:
            logger.error(f"invoke qids {e}")
        if not rqids:
            return qids
        qids.extend(rqids)
        return set(qids)

    async def pop(self, qid=None):
        qsize = self.mq.qsize(qid)
        if qsize > 0:
            r = self.mq.pop(qid)
            logger.debug(
                f"dequeue qid:{qid} qsize:{qsize-1} mq_size:{len(self.mq)} {r}"
            )
            return r
        if self.closing():
            return None
        try:
            return await self.rq.pop(qid)
        except Exception as e:
            logger.error(f"invoke pop {qid} {e}")

    def push(self, request):
        self.mq.push(request)
        logger.debug(f"enqueue mq_size:{len(self.mq)} {request}")

    def closing(self) -> bool:
        return bool(self.crawler.engine.slot.closing)

    def __len__(self):
        l = len(self.mq)
        if l > 0:
            return l
        if self.closing():
            return 0
        return 1

    def close(self):
        self.mq.close()

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        mq = MemoryRequestQueue()
        assert "RQ_API" in settings, "RQ_API not configured"
        api = settings.get("RQ_API")
        timeout = settings.getfloat("RQ_API_TIMEOUT", 3)
        logger.debug(f"RQ_API:{api} timeout:{timeout}")
        rq = HTTPRequestQueue(api, timeout)
        return cls(crawler, mq, rq)
