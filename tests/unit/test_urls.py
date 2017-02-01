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


def test_clean_webpage_links():
    assert urls.clean_webpage_links(' foo ') == 'foo'
    assert urls.clean_webpage_links(' foo\t ') == 'foo'

    assert urls.clean_webpage_links('\x01 foo ') == 'foo'

    assert urls.clean_webpage_links('///foo ') == '/foo'
    assert urls.clean_webpage_links('////foo ') == '/foo'

    # XXX tests for embedded spaces etc.


def test_special_seed_handling():
    assert urls.special_seed_handling('foo') == 'http://foo'
    assert urls.special_seed_handling('//foo') == 'http://foo'
    assert urls.special_seed_handling('https://foo') == 'https://foo'
    assert urls.special_seed_handling('mailto:foo') == 'mailto:foo'


def test_safe_url_canonicalization():
    assert urls.safe_url_canonicalization('http://example.com/?') == ('http://example.com/', '')
    assert urls.safe_url_canonicalization('http://example.com/?foo=bar') == \
        ('http://example.com/?foo=bar', '')
    assert urls.safe_url_canonicalization('HTTP://EXAMPLE.COM/') == ('http://example.com/', '')
    assert urls.safe_url_canonicalization('HTTP://EXAMPLE.COM:80/') == ('http://example.com/', '')
    assert urls.safe_url_canonicalization('httpS://EXAMPLE.COM:443/') == ('https://example.com/', '')
    assert urls.safe_url_canonicalization('HTTP://EXAMPLE.COM:81/') == ('http://example.com:81/', '')
    assert urls.safe_url_canonicalization('%2a%3Doof%20%%2f') == ('%2A%3Doof%20%%2F', '')
    assert urls.safe_url_canonicalization('foo%2a%3D%20%%2ffoo') == ('foo%2A%3D%20%%2Ffoo', '')
    assert urls.safe_url_canonicalization('http://example.com#frag') == ('http://example.com', '#frag')
    assert urls.safe_url_canonicalization('http://example.com#!frag') == ('http://example.com', '#!frag')
    assert urls.safe_url_canonicalization('http://example.com/#frag') == ('http://example.com/', '#frag')
    assert urls.safe_url_canonicalization('http://example.com/?foo=bar#frag') == \
        ('http://example.com/?foo=bar', '#frag')
    assert urls.safe_url_canonicalization('%2g') == ('%2g', '')


def test_special_redirect():
    assert urls.special_redirect(URL('foo'), URL('bar')) is None
    assert urls.special_redirect(URL('http://example.com/'), URL('http://example.com/foo')) is None
    assert urls.special_redirect(URL('http://example.com/'), URL('https://example.com/foo')) is None
    assert urls.special_redirect(URL('http://example.com/'), URL('https://www.example.com/foo')) is None
    assert urls.special_redirect(URL('http://example.com/'), URL('http://example.com/?foo=1')) is None
    assert urls.special_redirect(URL('http://example.com/'), URL('http://example.com/bar?foo=1')) is None
    url1 = URL('http://example.com/')
    assert urls.special_redirect(url1, url1) == 'same'
    assert urls.special_redirect(url1, URL('https://example.com/')) == 'tohttps'
    assert urls.special_redirect(url1, URL('http://www.example.com/')) == 'towww'
    assert urls.special_redirect(url1, URL('https://www.example.com/')) == 'towww+tohttps'

    url2 = URL('http://www.example.com/')
    assert urls.special_redirect(url2, URL('https://www.example.com/')) == 'tohttps'
    assert urls.special_redirect(url2, URL('http://example.com/')) == 'tononwww'
    assert urls.special_redirect(url2, URL('https://example.com/')) == 'tononwww+tohttps'

    url3 = URL('https://www.example.com/')
    assert urls.special_redirect(url3, URL('http://www.example.com/')) == 'tohttp'
    assert urls.special_redirect(url3, URL('https://example.com/')) == 'tononwww'
    assert urls.special_redirect(url3, URL('http://example.com/')) == 'tononwww+tohttp'

    url4 = URL('https://example.com/')
    assert urls.special_redirect(url4, URL('http://www.example.com/')) == 'towww+tohttp'


def test_get_domain():
    assert urls.get_domain('http://www.bbc.co.uk') == 'bbc.co.uk'
    assert urls.get_domain('http://www.nhs.uk') == 'www.nhs.uk'  # nhs.uk is a public suffix, surprise
    assert urls.get_domain('http://sub.nhs.uk') == 'sub.nhs.uk'  # ditto
    assert urls.get_domain('http://www.example.com') == 'example.com'
    assert urls.get_domain('http://sub.example.com') == 'example.com'
    assert urls.get_domain('http://sub.blogspot.com') == 'sub.blogspot.com'  # we want this behavior
    # if the blogspot test doesn't work, try this from the shell: "tldextract -u -p"
    # unfortunately, all tldextract users use the same cache
    assert urls.get_domain('http://www.com') == 'www.com'


def test_get_hostname():
    assert urls.get_hostname('http://www.bbc.co.uk') == 'www.bbc.co.uk'
    assert urls.get_hostname('http://www.bbc.co.uk', remove_www=True) == 'bbc.co.uk'
    assert urls.get_hostname('http://bbc.co.uk') == 'bbc.co.uk'
    assert urls.get_hostname('http://www.example.com') == 'www.example.com'
    assert urls.get_hostname('http://www.example.com:80') == 'www.example.com:80'
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
    assert list(url.urlparse) == ['http', 'www.example.com', '/', '', '', '']
    assert url.netloc == 'www.example.com'
    assert url.hostname == 'www.example.com'
    assert url.hostname_without_www == 'example.com'
    assert url.registered_domain == 'example.com'
    assert url.original_frag is None
    url = URL('http://www.example.com/#foo#foo')
    assert url.original_frag == 'foo#foo'
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
    urlj = URL('http://www.example.com/foo/')
    url = URL('foo', urljoin=urlj)
    assert url.url == 'http://www.example.com/foo/foo'
    url = URL('/bar', urljoin=urlj)
    assert url.url == 'http://www.example.com/bar'
    url = URL('http://sub.example.com/', urljoin=urlj)
    assert url.url == 'http://sub.example.com/'

    # read-only
    with pytest.raises(AttributeError):
        url.url = 'foo'
