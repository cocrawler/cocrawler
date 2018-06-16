import tempfile
import os
import pytest

from cocrawler.urls import URL
import cocrawler.datalayer as datalayer
import cocrawler.config as config

def test_seen():
    c = {'Robots': {'RobotsCacheSize': 1, 'RobotsCacheTimeout': 1}}
    config.set_config(c)
    dl = datalayer.Datalayer()
    assert not dl.crawled(URL('http://example.com'))
    dl.add_crawled(URL('http://example.com'))
    assert dl.crawled(URL('http://example.com'))


def test_robotscache():
    c = {'Robots': {'RobotsCacheSize': 1, 'RobotsCacheTimeout': 1}}
    config.set_config(c)
    dl = datalayer.Datalayer()
    with pytest.raises(KeyError):
        dl.read_robots_cache('http://example.com')
    dl.cache_robots('http://example.com', b'THIS IS A TEST')
    assert dl.read_robots_cache('http://example.com') == b'THIS IS A TEST'


def test_saveload():
    tf = tempfile.NamedTemporaryFile(delete=False)
    name = tf.name

    c = {'Robots': {'RobotsCacheSize': 1, 'RobotsCacheTimeout': 1}}
    config.set_config(c)
    dl = datalayer.Datalayer()
    dl.add_crawled(URL('http://example.com'))
    assert dl.crawled(URL('http://example.com'))

    with open(name, 'wb') as f:
        dl.save(f)
    dl.add_crawled(URL('http://example2.com'))
    with open(name, 'rb') as f:
        dl.load(f)

    assert dl.crawled(URL('http://example.com'))
    assert not dl.crawled(URL('http://example2.com'))
    os.unlink(name)
    assert not os.path.exists(name)


def test_summarize(capsys):
    c = {'Robots': {'RobotsCacheSize': 1, 'RobotsCacheTimeout': 1}}
    config.set_config(c)
    dl = datalayer.Datalayer()
    dl.add_crawled(URL('http://example.com'))
    dl.add_crawled(URL('http://example2.com'))
    dl.summarize()

    out, err = capsys.readouterr()

    assert len(err) == 0
    assert out.startswith('2 crawled')
