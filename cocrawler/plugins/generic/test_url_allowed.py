import pytest
import urllib.parse

import url_allowed

def test_get_domain():
    assert url_allowed.get_domain('http://www.bbc.co.uk') == 'bbc.co.uk'
    assert url_allowed.get_domain('http://www.nhs.uk') == 'nhs.uk' # nhs.uk is a public suffix!
    assert url_allowed.get_domain('http://www.example.com') == 'example.com'
    assert url_allowed.get_domain('http://sub.example.com') == 'example.com'

def test_gethostname():
    assert url_allowed.get_hostname('http://www.bbc.co.uk') == 'bbc.co.uk'
    assert url_allowed.get_hostname('http://www.example.com') == 'example.com'
    assert url_allowed.get_hostname('http://www.example.com:80') == 'example.com:80'
    assert url_allowed.get_hostname('http://bbc.co.uk') == 'bbc.co.uk'
    assert url_allowed.get_hostname('http://www.sub.example.com') == 'sub.example.com'
    assert url_allowed.get_hostname('http://sub.example.com') == 'sub.example.com'

def test_url_allowed():
    assert not url_allowed.url_allowed('ftp://example.com')
    url_allowed.SEEDS.add('example.com')
    url_allowed.POLICY = 'SeedsDomain'
    assert url_allowed.url_allowed('http://example.com')
    assert url_allowed.url_allowed('http://sub.example.com')
    url_allowed.POLICY = 'SeedsHostname'
    assert not url_allowed.url_allowed('http://sub.example.com')
    url_allowed.POLICY = 'OnlySeeds'
    assert not url_allowed.url_allowed('http://example.com')
    url_allowed.POLICY = 'AllDomains'
    assert url_allowed.url_allowed('http://example.com')
    assert url_allowed.url_allowed('http://exa2mple.com')
    assert url_allowed.url_allowed('http://exa3mple.com')

def test_scheme_allowed():
    assert url_allowed.scheme_allowed(urllib.parse.urlparse('http://example.com'))
    assert url_allowed.scheme_allowed(urllib.parse.urlparse('https://example.com'))
    assert not url_allowed.scheme_allowed(urllib.parse.urlparse('ftp://example.com'))

def test_extension_allowed():
    assert url_allowed.extension_allowed(urllib.parse.urlparse('https://example.com/'))
    assert url_allowed.extension_allowed(urllib.parse.urlparse('https://example.com/thing.with.dots/'))
    assert url_allowed.extension_allowed(urllib.parse.urlparse('https://example.com/thing.with.dots'))
    assert url_allowed.extension_allowed(urllib.parse.urlparse('https://example.com/index.html'))
    assert not url_allowed.extension_allowed(urllib.parse.urlparse('https://example.com/foo.jpg'))
    assert not url_allowed.extension_allowed(urllib.parse.urlparse('https://example.com/foo.tar.gz'))
