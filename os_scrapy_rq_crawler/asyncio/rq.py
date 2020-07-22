import logging

from os_scrapy_rq_crawler.utils import HTTPRequestQueue, MemoryRequestQueue

logger = logging.getLogger(__name__)


class AsyncRequestQueue(object):
    def __init__(self, crawler, mq, rq):
        self.crawler = crawler
        self.mq = mq
        self.rq = rq

    async def qids(self, k=16):
        m = int(k / 2)
        qids = [] if len(self.mq) <= 0 else self.mq.qids(m if m else 1)
        if self.closing():
            return qids
        r = k - len(qids)
        if r <= 0:
            return qids
        rqids = []
        try:
            rqids = await self.rq.qids(r)
        except Exception as e:
            logger.error(f"qids {e}")
        if not rqids:
            return qids
        qids.extend(rqids)
        return set(qids)

    async def pop(self, qid=None):

        if self.mq.qsize(qid) > 0:
            return self.mq.pop(qid)
        if self.closing():
            return None

        return await self.rq.pop(qid)

    def push(self, request):
        self.mq.push(request)

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
