import asyncio
import logging
import random
import time
from collections import namedtuple
from urllib.parse import urljoin, urlparse

import aiohttp
import async_timeout
from queuelib import queue
from scrapy.http.request import Request
from scrapy.utils.defer import maybeDeferred_coro
from twisted.internet import defer

try:
    import ujson as json
except:
    import json


def as_future(d):
    return d.asFuture(asyncio.get_event_loop())


def as_deferred(f):
    return defer.Deferred.fromFuture(asyncio.ensure_future(f))


async def queues_from_rq(api, k=16, timeout=0):
    status, ret, api_url = await raw_queues_from_rq(api, k, timeout)
    if status == 200:
        ret = queues_from_json(ret)
    return status, ret, api_url


async def request_from_rq(
    api,
    qid,
    timeout=0,
    callback=None,
    errback=None,
    dont_filter=True,
    priority=0,
    flags=None,
    cb_kwargs=None,
):
    status, ret, api_url = await raw_request_from_rq(api, qid, timeout=timeout)
    if status == 200:
        ret = request_from_json(
            ret,
            callback=callback,
            errback=errback,
            dont_filter=dont_filter,
            priority=priority,
            flags=flags,
            cb_kwargs=cb_kwargs,
        )
    return status, ret, api_url


async def post_rq(request_url, timeout=0):
    async with aiohttp.ClientSession() as session:
        with async_timeout.timeout(timeout):
            async with session.post(request_url) as response:
                status = response.status
                text = await response.text()
                return status, text.strip()


async def raw_request_from_rq(api, qid, timeout=0):
    api_url = urljoin(api, "queue/dequeue/?q=%s" % qid)
    status, ret = await post_rq(api_url, timeout=timeout)
    return status, ret, api_url


async def raw_queues_from_rq(api, k=16, timeout=0):
    api_url = urljoin(api, "queues/?k=%d" % k)
    status, ret = await post_rq(api_url, timeout=timeout)
    return status, ret, api_url


def queues_from_json(queues_json):
    return json.loads(queues_json)


def request_from_json(
    request_json,
    callback=None,
    errback=None,
    dont_filter=True,
    priority=0,
    flags=None,
    cb_kwargs=None,
):
    r = json.loads(request_json)
    return Request(
        url=r["url"],
        method=r.get("method", "GET"),
        meta=r.get("meta", None),
        headers=r.get("headers", None),
        cookies=r.get("cookies", None),
        body=r.get("body", None),
        encoding=r.get("encoding", "utf-8"),
        dont_filter=dont_filter,
        callback=callback,
        errback=errback,
        priority=priority,
        flags=flags,
        cb_kwargs=cb_kwargs,
    )


class Pool(object):
    def __init__(self, size):
        assert size > 0
        self.pool = asyncio.Queue()
        self.count = 0
        self.size = size
        self.paused = asyncio.Event()

    def pause(self):
        self.paused.clear()

    def unpause(self):
        self.paused.set()

    def maybeDeferred(self, f, *args, **kwargs):
        return maybeDeferred_coro(self.maybeFuture(f, *args, **kwargs))

    def maybeFuture(self, f, *args, **kwargs):
        async def _call():
            if self.count < self.size:
                self.count += 1
                try:
                    await self.pool.put(self.count)
                except asyncio.CancelledError:
                    self.count -= 1
                    raise
            try:
                q = await self.pool.get()
            except asyncio.CancelledError:
                raise

            try:
                await self.paused.wait()
                r = await as_future(maybeDeferred_coro(f, *args, **kwargs))
            finally:
                self.pool.task_done()
                await asyncio.shield(self.pool.put(q))
            return r

        return asyncio.ensure_future(_call())


DEFAULT_SCHEME_PORT = {
    "http": "80",
    "https": "443",
    "ftp": "21",
    "ssh": "22",
}


class QueueID(namedtuple("QueueID", "host port scheme")):
    def __str__(self):
        return ":".join(self)

    __repr__ = __str__


def qid_from_request(request: Request):
    if "rq.qid" in request.meta:
        return request.meta["rq.qid"]
    return qid_from_url(request.url)


def qid_from_string(s: str):
    c = s.split(":")
    assert len(c) == 3
    return QueueID(*c)


def qid_from_url(url: str):
    t = url.find("/", 8)
    if t > 0:
        url = url[0 : t + 1]
    p = urlparse(url)
    host = p.hostname if p.hostname else ""
    port = str(p.port) if p.port is not None else ""
    scheme = p.scheme
    if scheme in DEFAULT_SCHEME_PORT:
        if DEFAULT_SCHEME_PORT[scheme] == port:
            port = ""
    return QueueID(host, port, scheme)


def class_fullname(cls):
    return f"{cls.__module__}.{cls.__name__}"


class MemoryRequestQueue(object):
    def __init__(self):
        self._queues = {}
        self._num = 0
        self.logger = logging.getLogger(self.__class__.__name__)

    def qids(self, k=16):
        return random.sample(self._queues.keys(), min(k, len(self._queues)))

    def qsize(self, qid):
        if qid not in self._queues:
            return 0
        return len(self._queues[qid])

    def push(self, request):
        qid = qid_from_request(request)
        if qid not in self._queues:
            self._queues[qid] = queue.FifoMemoryQueue()
        q = self._queues[qid]
        f = q.push
        if "rq.enqueue" in request.meta and request.meta["rq.enqueue"] == "lifo":
            f = q.q.appendleft
        f(request)
        self._num += 1

    def pop(self, qid=None):
        s = time.time()
        r = self._pop(qid)
        self.logger.debug(f"pop from {qid} {time.time()-s:.5f} {r}")
        return r

    def _pop(self, qid=None):
        if not self._queues:
            return None
        if qid is None:
            qid = random.choice(list(self._queues.keys()))
        elif qid not in self._queues:
            return None
        queue = self._queues[qid]
        request = queue.pop()
        if len(queue) <= 0:
            del self._queues[qid]
            queue.close()
        self._num -= 1
        return request

    def __len__(self):
        return self._num

    def close(self):
        active = []
        for p, q in self._queues.items():
            active.append(p)
            q.close()
        return active


class HTTPRequestQueue(object):
    def __init__(self, api, timeout=0):
        self.api = api
        self.timeout = timeout
        self.logger = logging.getLogger(self.__class__.__name__)

    async def qids(self, k=16):
        status, ret, api_url = await queues_from_rq(self.api, k, self.timeout)
        if status == 200:
            qids = []
            for q in ret["queues"]:
                qids.append(qid_from_string(q["qid"]))
            return qids

    async def pop(self, qid):
        def _log(f, s, m):
            f(f"pop from {qid} {time.time()-s:.5f} {m}")

        s = time.time()
        try:
            status, ret, api_url = await request_from_rq(
                self.api, str(qid), self.timeout
            )
            if status == 200:
                _log(self.logger.debug, s, ret)
                return ret
            _log(self.logger.warning, s, f"{status} {ret}")
        except Exception as e:
            _log(self.logger.error, s, e)

    def push(self, request):
        pass

    def __len__(self):
        return 1

    def close(self):
        pass


def cancel_futures(futures):
    dlist = []
    for future in futures:
        if not future.done() and not future.cancelled():
            future.cancel()
        dlist.append(maybeDeferred_coro(lambda: future))
    return defer.DeferredList(dlist)
