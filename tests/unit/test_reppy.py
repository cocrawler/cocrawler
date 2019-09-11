import pytest

from reppy.robots import Robots


def test_cocrawler_reppy():
    r1 = Robots.parse('http://example.com/robots.txt', '''
User-Agent: foo
Allow: /
# comment
Disallow: /
Disallow: /disallowed
''')
    r2 = Robots.parse('http://example.com/robots.txt', '''
User-Agent: foo
Allow: /

Disallow: /
Disallow: /disallowed
''')
    r3 = Robots.parse('', '''
User-Agent: foo
Allow: /

Disallow: /
Disallow: /disallowed
''')

    # despite the blank line or comment, 'foo' is disllowed from disallowed
    assert r1.allowed('/', 'foo') is True
    assert r1.allowed('/disallowed', 'foo') is False
    assert r2.allowed('/', 'foo') is True
    assert r2.allowed('/disallowed', 'foo') is False
    assert r3.allowed('/', 'foo') is True
    assert r3.allowed('/disallowed', 'foo') is False

    # blank line does not reset user-agent to *, so bar has no rules
    assert r1.allowed('/', 'bar') is True
    assert r1.allowed('/disallowed', 'bar') is True
    assert r2.allowed('/', 'bar') is True
    assert r2.allowed('/disallowed', 'bar') is True
    assert r3.allowed('/', 'bar') is True
    assert r3.allowed('/disallowed', 'bar') is True

    # no substring weirdnesses, so foobar does not match foo rules
    assert r1.allowed('/', 'foobar') is True
    assert r1.allowed('/disallowed', 'foobar') is True
    assert r2.allowed('/', 'foobar') is True
    assert r2.allowed('/disallowed', 'foobar') is True
    assert r3.allowed('/', 'foobar') is True
    assert r3.allowed('/disallowed', 'foobar') is True


@pytest.mark.xfail(reason='https://github.com/seomoz/reppy/issues/113', strict=True)
def test_cocrawler_reppy_xfail():
    r4 = Robots.parse('', '''
User-agent: *
Disallow: //
''')

    # ibm.com, I'm looking at you
    assert r4.allowed('/foo', '') is True
    assert r4.allowed('/', '') is True
