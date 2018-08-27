import urllib.parse

import pytest
import tldextract

import cocrawler.urls as urls
from cocrawler.urls import URL


def test_urllib_parse():
    # This is just here so I can understand what urllib is doing with these:
    assert urllib.parse.urljoin('http://example.com/foo', '///bar') == 'http://example.com/bar'
    assert urllib.parse.urljoin('http://example.com/foo', '////bar') == 'http://example.com//bar'
    assert urllib.parse.urljoin('http://example.com/foo', '/////bar') == 'http://example.com///bar'

    assert urllib.parse.urljoin('https://example.com/foo', '///bar') == 'https://example.com/bar'
    assert urllib.parse.urljoin('https://example.com/foo', '////bar') == 'https://example.com//bar'
    assert urllib.parse.urljoin('https://example.com/foo', '/////bar') == 'https://example.com///bar'

    assert urllib.parse.urljoin('http://example.com/foo', '///bar.com') == 'http://example.com/bar.com'
    assert urllib.parse.urljoin('http://example.com/foo', '////bar.com') == 'http://example.com//bar.com'
    assert urllib.parse.urljoin('http://example.com/foo', '/////bar.com') == 'http://example.com///bar.com'

    # this round-trips; I canonicalize these urls the same
    assert urllib.parse.urljoin('http://example.com', '?q=123') == 'http://example.com?q=123'
    assert urllib.parse.urljoin('http://example.com/', '?q=123') == 'http://example.com/?q=123'


def test_clean_webpage_links():
    cwl = urls.clean_webpage_links
    assert cwl(' foo ') == 'foo'
    assert cwl(' foo\t ') == 'foo'

    assert cwl('\x01 foo ') == 'foo'

    assert cwl('///foo ') == '//foo'
    assert cwl('////foo ') == '//foo'
    assert cwl('http:///foo/bar') == 'http://foo/bar'
    assert cwl('https:///foo\\bar') == 'https://foo/bar'
    assert cwl('\\\\\\foo ') == '//foo'
    assert cwl('\\\\\\\\foo ') == '//foo'
    assert cwl('http:\\\\\\foo') == 'http://foo'
    assert cwl('https:\\\\\\foo\\bar') == 'https://foo/bar'

    assert cwl('h\nt\r\ntp://ex\r\nample.com') == 'http://example.com'

    # short urls don't mess with this
    assert cwl('"') == '"'
    assert cwl('http://foo.com">') == 'http://foo.com">'  # although maybe this should flag
    # long urls
    assert cwl('x'*100 + ' ' + 'x'*400) == 'x' * 100
    assert cwl('x'*100 + '\r >"' + 'x'*400) == 'x' * 100
    assert cwl('x'*100 + '\n' + 'x'*400) == 'x' * 100
    assert cwl('x'*2001) == ''  # throw-up-hands error case

    assert cwl('&amp;') == '&'
    assert cwl('&amp;amp;') == '&amp;'
    assert cwl('&#038;') == '&'


def test_remove_dot_segments():
    rds = urls.remove_dot_segments
    # examples from rfc 3986
    assert rds('/a/b/c/./../../g') == '/a/g'
    assert rds('/mid/content=5/../6') == '/mid/6'

    # and a few test cases of our own
    assert rds('foo') == 'foo'  # we used to raise ValueError, but it's too common
    assert rds('/') == '/'
    assert rds('/..') == '/'
    assert rds('/.') == '/'
    assert rds('/../foo') == '/foo'
    assert rds('/../foo/') == '/foo/'
    assert rds('/.././foo/./') == '/foo/'
    assert rds('/./.././../foo/') == '/foo/'
    assert rds('/./.././../foo/./') == '/foo/'
    assert rds('/./.././../foo/../bar/') == '/bar/'
    assert rds('/./.././../foo/../bar') == '/bar'

    # urljoin examples from RFC 3986 -- joined 'by hand' and then ./.. processed
    # kept only the ones with ./..
    assert rds('/b/c/./g') == '/b/c/g'
    assert rds('/b/c/.') == '/b/c'
    assert rds('/b/c/./') == '/b/c/'
    assert rds('/b/c/..') == '/b'
    assert rds('/b/c/../') == '/b/'
    assert rds('/b/c/../g') == '/b/g'
    assert rds('/b/c/../..') == '/'
    assert rds('/b/c/../../') == '/'
    assert rds('/b/c/../../g') == '/g'


def test_safe_url_canonicalization():
    suc = urls.safe_url_canonicalization
    assert suc('http://example.com/?') == ('http://example.com/', '')
    assert suc('http://Example%2ECom?') == ('http://example.com/', '')
    assert suc('http://example.com/?foo=bar') == ('http://example.com/?foo=bar', '')
    assert suc('http://example.com?foo=bar') == ('http://example.com/?foo=bar', '')
    assert suc('HTTP://EXAMPLE.COM/') == ('http://example.com/', '')
    assert suc('HTTP://EXAMPLE.COM:80/') == ('http://example.com/', '')
    assert suc('httpS://EXAMPLE.COM:443/') == ('https://example.com/', '')
    assert suc('HTTP://EXAMPLE.COM:81/') == ('http://example.com:81/', '')
    assert suc('http://example.com#frag') == ('http://example.com/', '#frag')
    assert suc('http://example.com#!frag') == ('http://example.com/', '#!frag')
    assert suc('http://example.com/#frag') == ('http://example.com/', '#frag')
    assert suc('http://example.com/?foo=bar#frag') == ('http://example.com/?foo=bar', '#frag')

    assert suc('http://bücher.com/?') == ('http://xn--bcher-kva.com/', '')

    assert suc('http://example.com/%2a%3Doof%20%%2f') == ('http://example.com/*=oof%20%%2f', '')
    assert suc('http://example.com/foo%2a%3D%20%%2ffoo') == ('http://example.com/foo*=%20%%2ffoo', '')

    # unreserved
    assert suc('http://example.com/%41%5a%61%7a%30%39%2d%2e%5f%7e') == ('http://example.com/AZaz09-._~', '')
    assert suc('http://example.com/%5b%7b%3C') == ('http://example.com/%5B%7B%3C', '')

    # path
    assert suc('http://example.com/%21%24%3b%3d%3a%40') == ('http://example.com/!$;=:@', '')
    assert suc('http://example.com/?%21%24%3b%3d%3a%40') == ('http://example.com/?!$;%3D:@', '')
    assert suc('http://example.com/#%21%24%3b%3d%3a%40') == ('http://example.com/', '#!$;%3D:@')
    assert suc('http://example.com/foo bar') == ('http://example.com/foo%20bar', '')

    # query/fragment
    assert suc('http://example.com/%3a%40%2f%3f%40') == ('http://example.com/:@%2F%3F@', '')
    assert suc('http://example.com/?%3a%40%2f%3f%40') == ('http://example.com/?:@/?@', '')
    assert suc('http://example.com/?foo bar') == ('http://example.com/?foo+bar', '')
    assert suc('http://example.com/#%3a%40%2f%3f%40') == ('http://example.com/', '#:@/?@')

    # Bug report from Stbastian Nagel of CC to IA:
    assert suc('http://visit.webhosting.yahoo.com/visit.gif?&r=http%3A//web.archive.org/web/20090517140029/http%3A//anthonystewarthead.electric-chi.com/&b=Netscape%205.0%20%28Windows%3B%20en-US%29&s=1366x768&o=Win32&c=24&j=true&v=1.2') == \
        ('http://visit.webhosting.yahoo.com/visit.gif?&r=http://web.archive.org/web/20090517140029/http://anthonystewarthead.electric-chi.com/&b=Netscape%205.0%20(Windows;%20en-US)&s=1366x768&o=Win32&c=24&j=true&v=1.2', '')


def test_special_redirect():
    sr = urls.special_redirect
    assert sr(URL('http://example.com/'), URL('http://example.com/foo')) is None
    assert sr(URL('http://example.com/'), URL('https://example.com/foo')) is None
    assert sr(URL('http://example.com/'), URL('https://www.example.com/foo')) is None
    assert sr(URL('http://example.com/'), URL('http://example.com/?foo=1')) is None
    assert sr(URL('http://example.com/'), URL('http://example.com/bar?foo=1')) is None
    url1 = URL('http://example.com/')
    assert sr(url1, url1) == 'same'
    assert sr(url1, URL('https://example.com/')) == 'tohttps'
    assert sr(url1, URL('http://www.example.com/')) == 'towww'
    assert sr(url1, URL('https://www.example.com/')) == 'towww+tohttps'

    url2str = 'http://www.example.com/'
    url2 = URL(url2str)
    assert sr(url2, URL('https://www.example.com/')) == 'tohttps'
    assert sr(url2, URL('http://example.com/')) == 'tononwww'
    assert sr(url2, URL('https://example.com/')) == 'tononwww+tohttps'
    assert sr(url2str, 'https://www.example.com/') == 'tohttps'
    assert sr(url2str, 'http://example.com/') == 'tononwww'
    assert sr(url2str, 'https://example.com/') == 'tononwww+tohttps'

    url3 = URL('https://www.example.com/')
    assert sr(url3, URL('http://www.example.com/')) == 'tohttp'
    assert sr(url3, URL('https://example.com/')) == 'tononwww'
    assert sr(url3, URL('http://example.com/')) == 'tononwww+tohttp'

    url4 = URL('https://example.com/')
    assert sr(url4, URL('http://www.example.com/')) == 'towww+tohttp'

    url5 = URL('https://example.com/foo')
    url6 = URL('https://example.com/foo/')
    assert sr(url5, url6) == 'addslash'
    assert sr(url6, url5) == 'removeslash'


def test_get_domain():
    assert urls.get_domain('http://www.bbc.co.uk') == 'bbc.co.uk'
    assert urls.get_domain('http://www.nhs.uk') == 'www.nhs.uk'  # nhs.uk is a public suffix, surprise
    assert urls.get_domain('http://sub.nhs.uk') == 'sub.nhs.uk'  # ditto
    assert urls.get_domain('http://www.example.com') == 'example.com'
    assert urls.get_domain('http://sub.example.com') == 'example.com'
    assert urls.get_domain('http://sub.blogspot.com') == 'sub.blogspot.com'  # we want this behavior
    # if the blogspot test doesn't work, try this from the shell: "tldextract -u -p"
    # unfortunately, all tldextract users use the same cache
    # https://github.com/john-kurkowski/tldextract/issues/66
    assert urls.get_domain('http://www.com') == 'www.com'


def test_get_hostname():
    assert urls.get_hostname('http://www.bbc.co.uk') == 'www.bbc.co.uk'
    assert urls.get_hostname('http://www.bbc.co.uk', remove_www=True) == 'bbc.co.uk'
    assert urls.get_hostname('http://bbc.co.uk') == 'bbc.co.uk'
    assert urls.get_hostname('http://www.example.com') == 'www.example.com'
    assert urls.get_hostname('http://www.example.com:80') == 'www.example.com'
    assert urls.get_hostname('http://www.sub.example.com') == 'www.sub.example.com'
    assert urls.get_hostname('http://sub.example.com') == 'sub.example.com'
    assert urls.get_hostname('http://www.com') == 'www.com'
    assert urls.get_hostname('http://www.com', remove_www=True) == 'www.com'


def test_tldextract():
    '''
    verify that tldextract parses just the netloc
    This is neither documented or tested by tldextract (!)
    '''
    assert tldextract.extract('example.com').registered_domain == 'example.com'
    assert tldextract.extract('www.example.com').registered_domain == 'example.com'


def test_URL():
    url = URL('http://www.example.com/')
    assert url.url == 'http://www.example.com/'
    assert list(url.urlsplit) == ['http', 'www.example.com', '/', '', '']
    assert url.netloc == 'www.example.com'
    assert url.hostname == 'www.example.com'
    assert url.hostname_without_www == 'example.com'
    assert url.registered_domain == 'example.com'
    assert url.original_frag is None
    url = URL('http://www.example.com/#foo#foo')
    assert url.original_frag == '#foo#foo'
    url = URL('http://www.example.com/#')
    assert url.original_frag is None

    # canonicalization
    url = URL('http://www.example.com/?')
    assert url.url == 'http://www.example.com/'
    url = URL('http://www.example.com')
    assert url.url == 'http://www.example.com/'
    url = URL('http://www.example.com/?#')
    assert url.url == 'http://www.example.com/'
    url = URL('http://www.example.com/foo')
    assert url.url == 'http://www.example.com/foo'
    url = URL('http://www.example.com/foo/')
    assert url.url == 'http://www.example.com/foo/'

    # urljoin
    urlj1 = URL('http://www.example.com/foo/')
    urlj2 = 'http://www.example.com/foo/'
    url = URL('foo', urljoin=urlj1)
    assert url.url == 'http://www.example.com/foo/foo'
    url = URL('foo', urljoin=urlj1)
    assert url.url == 'http://www.example.com/foo/foo'
    url = URL('/bar', urljoin=urlj1)
    assert url.url == 'http://www.example.com/bar'
    url = URL('/bar', urljoin=urlj2)
    assert url.url == 'http://www.example.com/bar'
    url = URL('http://sub.example.com/', urljoin=urlj1)
    assert url.url == 'http://sub.example.com/'
    url = URL('http://sub.example.com/', urljoin=urlj2)
    assert url.url == 'http://sub.example.com/'

    url = URL('foo', urljoin='http://example.com/subdir/')  # base can cause this
    assert url.url == 'http://example.com/subdir/foo'

    # read-only
    with pytest.raises(AttributeError):
        url.url = 'foo'

    # urljoin examples from RFC 3986 -- python takes care of . and ..
    urlj = URL('http://a/b/c/d;p?q')
    # assert URL('g:h', urljoin=urlj).url == 'g:h'  # absolute url missing hostname
    assert URL('g', urljoin=urlj).url == 'http://a/b/c/g'
    assert URL('./g', urljoin=urlj).url == 'http://a/b/c/g'
    assert URL('g/', urljoin=urlj).url == 'http://a/b/c/g/'
    assert URL('/g', urljoin=urlj).url == 'http://a/g'
    assert URL('//g', urljoin=urlj).url == 'http://g/'  # altered because I insist on the trailing /
    assert URL('?y', urljoin=urlj).url == 'http://a/b/c/d;p?y'
    assert URL('g?y', urljoin=urlj).url == 'http://a/b/c/g?y'
    assert URL('#s', urljoin=urlj).url == 'http://a/b/c/d;p?q'  # I drop the frag
    assert URL('g#s', urljoin=urlj).url == 'http://a/b/c/g'  # I drop the frag
    assert URL('g?y#s', urljoin=urlj).url == 'http://a/b/c/g?y'  # I drop the frag
    assert URL(';x', urljoin=urlj).url == 'http://a/b/c/;x'
    assert URL('g;x', urljoin=urlj).url == 'http://a/b/c/g;x'
    assert URL('g;x?y#s', urljoin=urlj).url == 'http://a/b/c/g;x?y'  # I drop the frag
    assert URL('', urljoin=urlj).url == 'http://a/b/c/d;p?q'
    assert URL('.', urljoin=urlj).url == 'http://a/b/c/'
    assert URL('./', urljoin=urlj).url == 'http://a/b/c/'
    assert URL('..', urljoin=urlj).url == 'http://a/b/'
    assert URL('../', urljoin=urlj).url == 'http://a/b/'
    assert URL('../g', urljoin=urlj).url == 'http://a/b/g'
    assert URL('../..', urljoin=urlj).url == 'http://a/'
    assert URL('../../', urljoin=urlj).url == 'http://a/'
    assert URL('../../g', urljoin=urlj).url == 'http://a/g'
