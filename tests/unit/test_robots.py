import pytest

import cocrawler.robots as robots
from cocrawler.robots import is_plausible_robots


def test_preprocess_robots():
    robots_txt = '''
foo
#bar

baz
'''
    ret = '''foo\nbaz\n'''
    assert robots.preprocess_robots(robots_txt, 'foo', {}) == (ret, False)

    robots_txt = 'User-AgEnT: cOcRaWlEr\nAllow: /'
    json_log = {}
    ret = 'User-AgEnT: cOcRaWlEr\nAllow: /\n'
    assert robots.preprocess_robots(robots_txt, 'CoCrAwLeR', json_log) == (ret, True)
    assert json_log == {'action-lines': 1, 'mentions-us': True, 'size': 30, 'user-agents': 1}

    assert robots.preprocess_robots('', 'foo', {}) == ('', False)
    assert robots.preprocess_robots('foo', 'foo', {}) == ('foo\n', False)


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


def test_robots():
    '''
    There's already end-to-end testing for the normal functionality.
    Exercise only the weird stuff here.
    '''
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
