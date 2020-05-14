# os-scrapy-rq-crawler

[![Build Status](https://www.travis-ci.org/cfhamlet/os-scrapy-rq-crawler.svg?branch=master)](https://www.travis-ci.org/cfhamlet/os-scrapy-rq-crawler)
[![codecov](https://codecov.io/gh/cfhamlet/os-scrapy-rq-crawler/branch/master/graph/badge.svg)](https://codecov.io/gh/cfhamlet/os-scrapy-rq-crawler)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/os-scrapy-rq-crawler.svg)](https://pypi.python.org/pypi/os-scrapy-rq-crawler)
[![PyPI](https://img.shields.io/pypi/v/os-scrapy-rq-crawler.svg)](https://pypi.python.org/pypi/os-scrapy-rq-crawler)

This project provide Crawler for RQ mode.


## Install

```
pip install os-scrapy-rq-crawler
```

## Usage

The origin scrapy framework can not config Crawler. You can use [os-scrapy](https://github.com/cfhamlet/os-scrapy) to config and run with your Crawler class.

You can run example spider directly in the project root path.

```
os-scrapy crawl -r asyncio example 
```

* set Crawler class in the settings.py file

    ```
    CRAWLER_CLASS = "os_scrapy_rq_crawler.asyncio.Crawler"
    ```

* set asyncio reactor

    becase of the asyncio.Crawler depends on python asyncio, you need to set the asyncio reactor

    ```
    TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
    ```

    you can also use ``-r asyncio`` of the os-scrapy command line option to specify asyncio reactor

* run with os-scrapy command line tool

    ```
    os-scrapy crawl example 
    ```

## Unit Tests

```
tox
```

## License

MIT licensed.
