import urllib.parse

import urls

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
    assert urls.special_seed_handling('foo') == 'http:///foo'
    assert urls.special_seed_handling('//foo') == 'http://foo'
    assert urls.special_seed_handling('https://foo') == 'https://foo'
    assert urls.special_seed_handling('mailto:foo') == 'mailto:foo'

def test_safe_url_canonicalization():
    assert urls.safe_url_canonicalization('http://example.com/?') == ('http://example.com/', '')
    assert urls.safe_url_canonicalization('http://example.com/?foo=bar') == ('http://example.com/?foo=bar', '')
    assert urls.safe_url_canonicalization('HTTP://EXAMPLE.COM/') == ('http://example.com/', '')
    assert urls.safe_url_canonicalization('HTTP://EXAMPLE.COM:80/') == ('http://example.com/', '')
    assert urls.safe_url_canonicalization('HTTP://EXAMPLE.COM:81/') == ('http://example.com:81/', '')
    assert urls.safe_url_canonicalization('%2a%3Doof%20%%2f') == ('%2A%3Doof%20%%2F', '')
    assert urls.safe_url_canonicalization('foo%2a%3D%20%%2ffoo') == ('foo%2A%3D%20%%2Ffoo', '')
    assert urls.safe_url_canonicalization('http://example.com#frag') == ('http://example.com', '#frag')
    assert urls.safe_url_canonicalization('http://example.com#!frag') == ('http://example.com', '#!frag')
    assert urls.safe_url_canonicalization('http://example.com/#frag') == ('http://example.com/', '#frag')
    assert urls.safe_url_canonicalization('http://example.com/?foo=bar#frag') == ('http://example.com/?foo=bar', '#frag')

