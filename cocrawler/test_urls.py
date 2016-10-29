import urllib.parse

import pytest
import tldextract

import urls
from urls import URL

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
    assert urls.safe_url_canonicalization('http://example.com/?foo=bar') == ('http://example.com/?foo=bar', '')
    assert urls.safe_url_canonicalization('HTTP://EXAMPLE.COM/') == ('http://example.com/', '')
    assert urls.safe_url_canonicalization('HTTP://EXAMPLE.COM:80/') == ('http://example.com/', '')
    assert urls.safe_url_canonicalization('httpS://EXAMPLE.COM:443/') == ('https://example.com/', '')
    assert urls.safe_url_canonicalization('HTTP://EXAMPLE.COM:81/') == ('http://example.com:81/', '')
    assert urls.safe_url_canonicalization('%2a%3Doof%20%%2f') == ('%2A%3Doof%20%%2F', '')
    assert urls.safe_url_canonicalization('foo%2a%3D%20%%2ffoo') == ('foo%2A%3D%20%%2Ffoo', '')
    assert urls.safe_url_canonicalization('http://example.com#frag') == ('http://example.com', '#frag')
    assert urls.safe_url_canonicalization('http://example.com#!frag') == ('http://example.com', '#!frag')
    assert urls.safe_url_canonicalization('http://example.com/#frag') == ('http://example.com/', '#frag')
    assert urls.safe_url_canonicalization('http://example.com/?foo=bar#frag') == ('http://example.com/?foo=bar', '#frag')
    assert urls.safe_url_canonicalization('%2g') == ('%2g', '')

def test_special_redirect():
    assert urls.special_redirect('foo', 'bar') == None
    assert urls.special_redirect('http://example.com/', 'http://example.com/foo') == None
    assert urls.special_redirect('http://example.com/', 'https://example.com/foo') == None
    assert urls.special_redirect('http://example.com/', 'https://www.example.com/foo') == None
    assert urls.special_redirect('http://example.com/', 'http://example.com/?foo=1') == None
    assert urls.special_redirect('http://example.com/', 'http://example.com/bar?foo=1') == None
    url1 = 'http://example.com/'
    parts1 = urllib.parse.urlparse(url1)
    assert urls.special_redirect(url1, url1) == 'same'
    assert urls.special_redirect(url1, 'https://example.com/', parts=parts1) == 'tohttps'
    assert urls.special_redirect(url1, 'http://www.example.com/', parts=parts1) == 'towww'
    assert urls.special_redirect(url1, 'https://www.example.com/', parts=parts1) == 'towww+tohttps'

    url2 = 'http://www.example.com/'
    parts2 = urllib.parse.urlparse(url2)
    assert urls.special_redirect(url2, 'https://www.example.com/', parts=parts2) == 'tohttps'
    assert urls.special_redirect(url2, 'http://example.com/', parts=parts2) == 'tononwww'
    assert urls.special_redirect(url2, 'https://example.com/', parts=parts2) == 'tononwww+tohttps'

    url3 = 'https://www.example.com/'
    parts3 = urllib.parse.urlparse(url3)
    assert urls.special_redirect(url3, 'http://www.example.com/', parts=parts3) == 'tohttp'
    assert urls.special_redirect(url3, 'https://example.com/', parts=parts3) == 'tononwww'
    assert urls.special_redirect(url3, 'http://example.com/', parts=parts3) == 'tononwww+tohttp'

    url4 = 'https://example.com/'
    parts4 = urllib.parse.urlparse(url4)
    assert urls.special_redirect(url4, 'http://www.example.com/', parts=parts4) == 'towww+tohttp'

def test_get_domain():
    assert urls.get_domain('http://www.bbc.co.uk')  == 'bbc.co.uk'
    assert urls.get_domain('http://www.nhs.uk') == 'www.nhs.uk' # nhs.uk is a public suffix, so this is expected
    assert urls.get_domain('http://sub.nhs.uk') == 'sub.nhs.uk' # ditto
    assert urls.get_domain('http://www.example.com') == 'example.com'
    assert urls.get_domain('http://sub.example.com') == 'example.com'
    assert urls.get_domain('http://sub.blogspot.com') == 'sub.blogspot.com' # we want this behavior
    # if the blogspot test doesn't work, try this from the shell: "tldextract -u -p"
    # unfortunately, all tldextract users use the same cache
    assert urls.get_domain('http://www.com') == 'www.com'

def test_get_hostname():
    assert urls.get_hostname('http://www.bbc.co.uk') == 'www.bbc.co.uk'
    parts = urllib.parse.urlparse('http://www.bbc.co.uk')
    assert urls.get_hostname(None, parts=parts) == 'www.bbc.co.uk'
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
    assert url._url == 'http://www.example.com/'
    assert list(url.urlparse) == ['http', 'www.example.com', '/', '', '', '']
    assert url.netloc == 'www.example.com'
    assert url.hostname == 'www.example.com'
    assert url.hostname_without_www == 'example.com'
    assert url.registered_domain == 'example.com'
