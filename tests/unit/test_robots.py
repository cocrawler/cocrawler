import pytest

import cocrawler.robots as robots
import cocrawler.config as config


def test_preprocess_robots():
    robots_txt = '''
foo
#bar

baz
'''
    ret = '''foo\nbaz\n'''
    assert robots.preprocess_robots(robots_txt) == ret

    assert robots.preprocess_robots('') == ''
    assert robots.preprocess_robots('foo') == 'foo\n'


def test_strip_bom():
    robots_txt = b'\xef\xbb\xbf'
    assert robots.strip_bom(robots_txt) == b''
    robots_txt = b'\xef\xbb\xbf  '
    assert robots.strip_bom(robots_txt) == b''
    robots_txt = b'\xef\xbb\xbf  <'
    assert robots.strip_bom(robots_txt) == b'<'

    robots_txt = b'\xfe\xff'
    assert robots.strip_bom(robots_txt) == b''
    robots_txt = b'\xfe\xff  \n \t \vwumpus'
    assert robots.strip_bom(robots_txt) == b'wumpus'

    robots_txt = b'\xff\xfe'
    assert robots.strip_bom(robots_txt) == b''


def test_robots():
    '''
    There's already end-to-end testing for the normal functionality.
    Exercise only the weird stuff here.
    '''
    config.set_config({'Robots': {'MaxTries': 4, 'MaxRobotsPageSize': 500000},
                       'Logging': {}})
    # XXX really I should use the defaults in config.py so that I don't have
    # to edit the above as I add mandatory args
    r = robots.Robots('foo', None, None)

    robots_txt = b'<'
    plausible, message = r.is_plausible_robots('example.com', robots_txt, 1.0)
    assert not plausible
    assert len(message)

    robots_txt = b''  # application/x-empty
    plausible, message = r.is_plausible_robots('example.com', robots_txt, 1.0)
    assert plausible
    assert not len(message)

    robots_txt = b'x'*1000001
    plausible, message = r.is_plausible_robots('example.com', robots_txt, 1.0)
    assert not plausible
    assert len(message)

    robots_txt = b'foo'
    plausible, message = r.is_plausible_robots('example.com', robots_txt, 1.0)
    assert plausible
    assert not len(message)

@pytest.mark.xfail
def test_magic():
    # magic is 3 milliseconds of cpu burn/call so this is currently comented out in robots.py
    r = robots.Robots('foo', None, None, {'Robots': {'MaxTries': 4}, 'Logging': {}})
    robots_txt = b'%PDF-1.3\n'
    assert not r.is_plausible_robots('example.com', robots_txt, 1.0)
