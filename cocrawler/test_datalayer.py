import tempfile
import os
import pytest

from urls import URL
import datalayer

def test_seen():
    dl = datalayer.Datalayer({'Robots':{'RobotsCacheSize':1, 'RobotsCacheTimeout': 1}})
    assert not dl.seen_url(URL('http://example.com'))
    dl.add_seen_url(URL('http://example.com'))
    assert dl.seen_url(URL('http://example.com'))

def test_robotscache():
    dl = datalayer.Datalayer({'Robots':{'RobotsCacheSize':1, 'RobotsCacheTimeout': 1}})
    with pytest.raises(KeyError):
        dl.read_robots_cache('http://example.com')
    dl.cache_robots('http://example.com', b'THIS IS A TEST')
    assert dl.read_robots_cache('http://example.com') == b'THIS IS A TEST'

def test_saveload():
    tf = tempfile.NamedTemporaryFile(delete=False)
    name = tf.name

    dl = datalayer.Datalayer({'Robots':{'RobotsCacheSize':1, 'RobotsCacheTimeout': 1}})
    dl.add_seen_url(URL('http://example.com'))
    assert dl.seen_url(URL('http://example.com'))

    with open(name, 'wb') as f:
        dl.save(f)
    dl.add_seen_url(URL('http://example2.com'))
    with open(name, 'rb') as f:
        dl.load(f)

    assert dl.seen_url(URL('http://example.com'))
    assert not dl.seen_url(URL('http://example2.com'))
    os.unlink(name)
    assert not os.path.exists(name)

def test_summarize(capsys):
    dl = datalayer.Datalayer({'Robots':{'RobotsCacheSize':1, 'RobotsCacheTimeout': 1}})
    dl.add_seen_url(URL('http://example.com'))
    dl.add_seen_url(URL('http://example2.com'))
    dl.summarize()

    out, err = capsys.readouterr()

    assert len(err) == 0
    assert out.startswith('2 seen_urls')
