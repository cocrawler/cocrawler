'''
Most testing of cocrawler is done by fake crawling, but there are a few things...
'''

import asyncio
import tempfile
import os

import cocrawler
import config

def test_cocrawler():
    conf = config.config(None, None, confighome=False)

    # ok, we have to get around the useragent checks
    conf['UserAgent']['MyPrefix'] = 'pytest'
    conf['UserAgent']['URL'] = 'http://example.com/pytest-test-cocrawler.py'

    loop = asyncio.get_event_loop()
    crawler = cocrawler.Crawler(loop, conf)

    crawler.add_url(0, 'http://example1.com/', seed=True)
    crawler.add_url(0, 'http://example2.com/', seed=True)
    crawler.add_url(0, 'http://example3.com/', seed=True)
    assert crawler.qsize == 3

    f = tempfile.NamedTemporaryFile(delete=False)
    name = f.name
    crawler.savequeues(name)
    assert crawler.qsize == 0

    crawler.add_url(0, 'http://example4.com/', seed=True)
    assert crawler.qsize == 1

    crawler.loadqueues(name)

    assert crawler.qsize == 3

    os.unlink(name)
    assert not os.path.exists(name)
