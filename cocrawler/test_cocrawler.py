'''
Most testing of cocrawler is done by fake crawling, but there are a few things...
'''

import asyncio
import tempfile
import os

import cocrawler
import conf
from urls import URL


def test_cocrawler(capsys):
    config = conf.config(None, None, confighome=False)

    # ok, we have to get around the useragent checks
    config['UserAgent']['MyPrefix'] = 'pytest'
    config['UserAgent']['URL'] = 'http://example.com/pytest-test-cocrawler.py'

    loop = asyncio.get_event_loop()
    crawler = cocrawler.Crawler(loop, config)

    crawler.add_url(0, URL('http://example1.com/'), seed=True)
    crawler.add_url(0, URL('http://example2.com/'), seed=True)
    crawler.add_url(0, URL('http://example3.com/'), seed=True)
    assert crawler.qsize == 3

    f = tempfile.NamedTemporaryFile(delete=False)
    name = f.name

    with open(name, 'wb') as f:
        crawler.save(f)
    assert crawler.qsize == 0

    crawler.add_url(0, URL('http://example4.com/'), seed=True)
    assert crawler.qsize == 1

    with open(name, 'rb') as f:
        crawler.load(f)

    assert crawler.qsize == 3

    os.unlink(name)
    assert not os.path.exists(name)

    # clear out the existing capture
    out, err = capsys.readouterr()

    crawler.summarize()

    out, err = capsys.readouterr()

    assert err == ''
    assert len(out) >= 242  # not a very good test, but at least it is something
