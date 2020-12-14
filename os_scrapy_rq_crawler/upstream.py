import logging
import random
from collections import namedtuple
from enum import Enum

import aiorwlock

from .asyncio.rq import AsyncRequestQueue
from .utils import HTTPRequestQueue, MemoryRequestQueue


class NamedQueueID(namedtuple("NamedQueueID", "name qid")):
    def __str__(self):
        return str(self.qid)

    @property
    def host(self):
        return self.qid.host

    @property
    def scheme(self):
        return self.qid.scheme

    @property
    def port(self):
        return self.qid.port


class Upstream(HTTPRequestQueue):
    def __init__(self, name, api, timeout=None):
        super(Upstream, self).__init__(api, timeout)
        self.name = name

    async def qids(self, k=16, timeout=None):
        qids = await super(Upstream, self).qids(k, timeout=timeout)
        if qids:
            return [NamedQueueID(self.name, qid) for qid in qids]


class Status(Enum):
    WORKING = 1
    RETRYING = 2
    PAUSED = 3


class UpstreamRequestQueue(object):
    def __init__(self, upstreams=None):
        self.upstreams = dict([(s, dict()) for s in Status])
        if upstreams:
            self.upstreams[Status.WORKING] = upstreams
        self.rwlock = aiorwlock.RWLock()
        self.logger = logging.getLogger(self.__class__.__name__)

    def _exist(self, name) -> bool:
        return any([name in ups for ups in self.upstreams.values()])

    def _upstream(self, name):
        for s, ups in self.upstreams.items():
            if name in ups:
                return ups[name]

    async def add_upstream(self, upstream) -> bool:
        async with self.rwlock.writer_lock:
            if self._exist(upstream.name):
                return False
            self.upstreams[Status.WORKING][upstream.name] = upstream
            return True

    async def pause_upstream(self, name):
        async with self.rwlock.writer_lock:
            if name in self.upstreams[Status.PAUSED]:
                return True
            upstream = None
            for s in (Status.WORKING, Status.RETRYING):
                if name in self.upstreams[s]:
                    upstream = self.upstreams[s].pop(name)
                    break
            if upstream is not None:
                self.upstreams[Status.PAUSED][name] = upstream
                return True
            return False

    async def delete_upstream(self, name):
        async with self.rwlock.writer_lock:
            for s in Status:
                if name in self.upstreams[s]:
                    self.upstreams[s].pop(name)
                    return True
            return False

    async def qids(self, k=16, name=None, timeout=None):
        async with self.rwlock.reader_lock:
            if name is None:
                ups = self.upstreams[Status.WORKING]
                if ups:
                    name = random.choice(list(ups.keys()))

            if name is not None:
                return await self._qids(name, k, timeout=timeout)

    async def _qids(self, name, k, timeout=None):
        upstream = self._upstream(name)
        if upstream is not None:
            return await upstream.qids(k, timeout=timeout)

    async def pop(self, qid, timeout=None):
        if isinstance(qid, NamedQueueID):
            async with self.rwlock.reader_lock:
                upstream = self._upstream(qid.name)
                if upstream is not None:
                    return await upstream.pop(qid.qid, timeout=timeout)

    def __len__(self):
        return 1

    async def close(self):
        pass


class MultiUpstreamRequestQueue(AsyncRequestQueue):
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        mq = MemoryRequestQueue()
        assert "RQ_API" in settings, "RQ_API not configured"
        apis = settings.get("RQ_API")
        timeout = settings.getfloat("RQ_API_TIMEOUT", 3)

        def init_from_list(upstreams, apis):
            c = 0
            for api in apis:
                if isinstance(api, str):
                    name = f"upsteam-{c}"
                    upstreams[name] = Upstream(name, api, timeout)
                elif isinstance(api, tuple):
                    upstreams[api[0]] = Upstream(api[0], api[1], timeout)
                c += 1

        if isinstance(apis, dict):
            apis = list(apis.items())
        elif isinstance(apis, str):
            apis = [apis]
            if "," in apis:
                apis = apis.split(",")
        upstreams = {}
        init_from_list(upstreams, apis)

        rq = UpstreamRequestQueue(upstreams)
        return cls(crawler, mq, rq)
