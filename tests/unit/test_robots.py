import pytest

import cocrawler.robots as robots
from cocrawler.robots import is_plausible_robots, robots_facets


def test_robots_facets():
    robots_txt = 'User-AgEnT: cOcRaWlEr\nAllow: /'
    json_log = {}
    robots_facets(robots_txt, 'CoCrAwLeR', json_log)
    assert json_log == {'action-lines': 1, 'mentions-us': True, 'size': 30, 'user-agents': 1}


def test_strip_bom():
    robots_txt = b'\xef\xbb\xbf'
    assert robots.strip_bom(robots_txt) == b''
    robots_txt = b'\xef\xbb\xbf  '
    assert robots.strip_bom(robots_txt) == b'  '

    robots_txt = b'\xfe\xff'
    assert robots.strip_bom(robots_txt) == b''
    robots_txt = b'\xfe\xfffoo'
    assert robots.strip_bom(robots_txt) == b'foo'

    robots_txt = b'\xff\xfe'
    assert robots.strip_bom(robots_txt) == b''


def test_is_plausible_robots():
    robots_txt = b'<'
    plausible, message = is_plausible_robots(robots_txt)
    assert not plausible
    assert len(message)

    robots_txt = b''  # application/x-empty
    plausible, message = is_plausible_robots(robots_txt)
    assert plausible
    assert not len(message)

    robots_txt = b'x'*1000001
    plausible, message = is_plausible_robots(robots_txt)
    assert not plausible
    assert len(message)

    robots_txt = b'foo'
    plausible, message = is_plausible_robots(robots_txt)
    assert plausible
    assert not len(message)

@pytest.mark.xfail
def test_magic():
    # magic is 3 milliseconds of cpu burn/call so this is currently comented out in robots.py
    robots_txt = b'%PDF-1.3\n'
    assert not is_plausible_robots(robots_txt)
