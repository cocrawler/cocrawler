from urls import URL

import url_allowed

def test_url_allowed():
    assert not url_allowed.url_allowed(URL('ftp://example.com'))
    url_allowed.SEEDS.add('example.com')
    url_allowed.POLICY = 'SeedsDomain'
    assert url_allowed.url_allowed(URL('http://example.com'))
    assert url_allowed.url_allowed(URL('http://www.example.com'))
    assert url_allowed.url_allowed(URL('http://sub.example.com'))
    url_allowed.POLICY = 'SeedsHostname'
    assert url_allowed.url_allowed(URL('http://example.com'))
    assert url_allowed.url_allowed(URL('http://www.example.com'))
    assert not url_allowed.url_allowed(URL('http://sub.example.com'))
    url_allowed.POLICY = 'OnlySeeds'
    assert not url_allowed.url_allowed(URL('http://example.com'))
    url_allowed.POLICY = 'AllDomains'
    assert url_allowed.url_allowed(URL('http://example.com'))
    assert url_allowed.url_allowed(URL('http://exa2mple.com'))
    assert url_allowed.url_allowed(URL('http://exa3mple.com'))

def test_scheme_allowed():
    assert url_allowed.scheme_allowed(URL('http://example.com'))
    assert url_allowed.scheme_allowed(URL('https://example.com'))
    assert not url_allowed.scheme_allowed(URL('ftp://example.com'))

def test_extension_allowed():
    assert url_allowed.extension_allowed(URL('https://example.com/'))
    assert url_allowed.extension_allowed(URL('https://example.com/thing.with.dots/'))
    assert url_allowed.extension_allowed(URL('https://example.com/thing.with.dots'))
    assert url_allowed.extension_allowed(URL('https://example.com/index.html'))
    assert not url_allowed.extension_allowed(URL('https://example.com/foo.jpg'))
    assert not url_allowed.extension_allowed(URL('https://example.com/foo.tar.gz'))
