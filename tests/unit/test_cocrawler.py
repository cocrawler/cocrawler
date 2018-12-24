'''
Most testing of cocrawler is done by fake crawling, but there are a few things...
'''

import tempfile
import os
import pytest

import cocrawler
import cocrawler.config as config
from cocrawler.urls import URL


@pytest.mark.asyncio
async def test_cocrawler(capsys):
    config.config(None, None)

    # we have to get around the useragent checks
    config.write('pytest', 'UserAgent', 'MyPrefix')
    config.write('http://example.com/pytest-test-cocrawler.py', 'UserAgent', 'URL')
    # and configure url_allowed
    config.write('AllDomains', 'Plugins', 'url_allowed')

    crawler = cocrawler.Crawler()

    crawler.add_url(0, {'url': URL('http://example1.com/')})
    crawler.add_url(0, {'url': URL('http://example2.com/')})
    crawler.add_url(0, {'url': URL('http://example3.com/')})

    assert crawler.qsize == 3

    f = tempfile.NamedTemporaryFile(delete=False)
    name = f.name

    with open(name, 'wb') as f:
        crawler.save(f)
    assert crawler.qsize == 0

    crawler.add_url(0, {'url': URL('http://example4.com/')})
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
    assert len(out) >= 200  # not a very good test, but at least it is something

    await crawler.close()  # needed for smooth shutdown
