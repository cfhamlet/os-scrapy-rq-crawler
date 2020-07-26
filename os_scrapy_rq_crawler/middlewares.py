# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy.downloadermiddlewares.redirect import (
    MetaRefreshMiddleware as MetaRfsMiddleware,
    RedirectMiddleware as RdrMiddleware,
)
from scrapy.http import Request

S_DOWNLOAD_DELAY = "download_delay"
S_DOWNLOAD_DELAY_RDR_ROLLBACK = S_DOWNLOAD_DELAY + "_rdr_rollback"


def process_response(request, response, spider):
    if isinstance(response, Request):
        if (
            S_DOWNLOAD_DELAY in request.meta
            and request.meta[S_DOWNLOAD_DELAY] is not None
        ):
            request.meta[S_DOWNLOAD_DELAY_RDR_ROLLBACK] = request.meta[S_DOWNLOAD_DELAY]
        request.meta[S_DOWNLOAD_DELAY] = None
    else:
        meta = request.meta
        if (
            S_DOWNLOAD_DELAY in meta
            and "redirect_urls" in meta
            and meta[S_DOWNLOAD_DELAY] is None
        ):
            meta.pop(S_DOWNLOAD_DELAY)
            if S_DOWNLOAD_DELAY_RDR in meta:
                meta[S_DOWNLOAD_DELAY] = meta.pop(S_DOWNLOAD_DELAY_RDR_ROLLBACK)
    return response


class RedirectMiddleware(RdrMiddleware):
    def process_response(self, request, response, spider):
        response = super(RedirectMiddleware, self).process_response(
            request, response, spider
        )
        return process_response(request, response, spider)


class MetaRefreshMiddleware(MetaRfsMiddleware):
    def process_response(self, request, response, spider):
        response = super(MetaRefreshMiddleware, self).process_response(
            request, response, spider
        )
        return process_response(request, response, spider)
