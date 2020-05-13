import asyncio
from urllib.parse import urljoin

import aiohttp
import async_timeout
from scrapy.http.request import Request
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
    api_url = urljoin(api, "queue/pop/?q=%s" % qid)
    status, ret = await post_rq(api_url, timeout=timeout)
    return status, ret, api_url


async def raw_queues_from_rq(api, k=10, timeout=0):
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
