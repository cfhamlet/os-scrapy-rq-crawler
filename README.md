# os-scrapy-rq-crawler

[![Build Status](https://www.travis-ci.org/cfhamlet/os-scrapy-rq-crawler.svg?branch=master)](https://www.travis-ci.org/cfhamlet/os-scrapy-rq-crawler)
[![codecov](https://codecov.io/gh/cfhamlet/os-scrapy-rq-crawler/branch/master/graph/badge.svg)](https://codecov.io/gh/cfhamlet/os-scrapy-rq-crawler)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/os-scrapy-rq-crawler.svg)](https://pypi.python.org/pypi/os-scrapy-rq-crawler)
[![PyPI](https://img.shields.io/pypi/v/os-scrapy-rq-crawler.svg)](https://pypi.python.org/pypi/os-scrapy-rq-crawler)

This project provide Crawler for RQ mode. Based on Scrapy 2.0+, require Python 3.6+

The [Scrapy](https://scrapy.org/) framework is used for crawling specific sites. It is not good for ["Broad Crawls"](https://docs.scrapy.org/en/latest/topics/broad-crawls.html). The Scrapy built-in schedule mechanism is not for many domains, it use one channel queue for requests of all different domains. The scheduler can not decide to crawl request of the specified domain.

The RQ mode is the key mechanism/concept for broad crawls. The key point is RQ(request queue), actually it is a banch of queues, requests of different domains in the different sub-queues. 

Deploy with the [os-rq-pod](https://github.com/cfhamlet/os-rq-pod) and [os-rq-hub](https://github.com/cfhamlet/os-rq-hub) you can build a large scalable distributed "Broad Crawls" system.

## Quick Start

We offer Crawler for RQ mode. Because the Scrapy framework can not custom Crawler class, so you can use [os-scrapy](https://github.com/cfhamlet/os-scrapy)(installed with this project) to start crawling. 

* install

    ```
    pip install os-scrapy-rq-crawler
    ```

* start your project 

    ```
    os-scrapy startproject <your_project>
    ```

* set Crawler class and [enable asyncio reactor](https://docs.scrapy.org/en/latest/topics/asyncio.html) in project setting.py

    ```
    CRAWLER_CLASS = "os_scrapy_rq_crawler.asyncio.Crawler"
    TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
    ```

* start crawling example spider

    ```
    os-scrapy crawl example
    ```

## Install

```
pip install os-scrapy-rq-crawler
```

When you already installed os-scrapy, you can run example spider directly in this project root path

```
os-scrapy crawl example
```


## Usage

This project offer Crawler class can be used in your project by config in settings.py file. And it can also be used as a scrapy project directly.

### Crawler Class

This project provide Crawler to enable RQ mode. The Scrapy framework can not config Crawler class, so you can use [os-scrapy](https://github.com/cfhamlet/os-scrapy) to specify

* in the settings.py

    ```
    CRAWLER_CLASS = "os_scrapy_rq_crawler.asyncio.Crawler"
    ```

* or with ``-c`` command line option

    ```
    os-scrapy crawl -c os_scrapy_rq_crawler.asyncio.Crawler example
    ```

Because the Crawler deponds on asyncio reactor, so you need to enable it

* in the settings.py

    ```
    TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
    ```

* or with ``-r`` command line option

    ```
    os-scrapy crawl -r asyncio example
    ```

### Request Queue

RQ is just a conception can be implements in different ways.

* config in the settings.py file

    ```
    SCHEDULER_REQUEST_QUEUE = "os_scrapy_rq_crawler.AsyncRequestQueue"
    ```

* ``os_scrapy_rq_crawler.MemoryRequestQueue``

    - the default requet queue
    - all requests in memory
    - the spider closed when no requests in the queue and [spider is idle](https://docs.scrapy.org/en/latest/topics/signals.html?highlight=idle#spider-idle)

* ``os_scrapy_rq_crawler.AsyncRequestQueue``

    - can be used with [os-rq-pod](https://github.com/cfhamlet/os-rq-pod) or [os-rq-hub](https://github.com/cfhamlet/os-rq-hub) as external request queue
    - the spider will not closed even if no requests to crawl
    - config the pod/hub http api in the settings.py

        ```
        RQ_API = "http://localhost:6789/api/"
        ```

    - config the api timeout

        ```
        RQ_API_TIMEOUT = 3.0
        ```

* ``os_scrapy_rq_crawler.MultiUpstreamRequestQueue``

    - same as ``os_scrapy_rq_crawler.AsyncRequestQueue``, can configure multi upstreams request queues

    - config the pod/hub http apis in the settings.py

        ```
        RQ_API = ["http://server01:6789/api/", "http://server02:6789/api/"]
        ```

### FYI

Our RQ mode Crawler is a substitute of the Scrapy built-in Crawler. Most of the Scrapy functionalities(middleware/extension) can also be used as normal.

There are some tips:

* the request queue is fifo, priority is not supported yet
* the requests come from downloader middlewares will push to the head of the request queue
* the request of the same domain will not crawl concurrently, ``CONCURRENT_REQUESTS`` control the max concurrency and ``DOWNLOAD_DELAY`` control the download dely
* you can set request delay: request.meta["download_delay"] = 3.0


## Unit Tests

```
sh scripts/test.sh
```

## License

MIT licensed.
