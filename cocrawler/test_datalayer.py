import pytest

import datalayer

dl = datalayer.Datalayer({'Robots':{'RobotsCacheSize':1, 'RobotsCacheTimeout': 1}})

def test_seen():
    assert not dl.seen_url('example.com')
    dl.add_seen_url('example.com')
    assert dl.seen_url('example.com')

def test_datalayer():
    with pytest.raises(KeyError):
        dl.read_robots_cache('http://example.com')
    dl.cache_robots('http://example.com', b'THIS IS A TEST')
    assert dl.read_robots_cache('http://example.com') == b'THIS IS A TEST'
