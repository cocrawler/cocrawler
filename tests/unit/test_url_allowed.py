from cocrawler.urls import URL
import cocrawler.url_allowed as url_allowed


def test_url_allowed():
    assert not url_allowed.url_allowed(URL('ftp://example.com'))

    url_allowed.setup(policy='SeedsDomain')
    url_allowed.setup_seeds([URL('http://example.com')])
    assert url_allowed.url_allowed(URL('http://example.com'))
    assert url_allowed.url_allowed(URL('http://www.example.com'))
    assert url_allowed.url_allowed(URL('http://sub.example.com'))

    url_allowed.setup(policy='SeedsHostname')
    url_allowed.setup_seeds([URL('http://example.com')])
    assert url_allowed.url_allowed(URL('http://example.com'))
    assert url_allowed.url_allowed(URL('http://www.example.com'))
    assert not url_allowed.url_allowed(URL('http://sub.example.com'))

    url_allowed.setup(policy='SeedsPrefix')
    url_allowed.setup_seeds([URL('http://example.com/prefix1')])
    url_allowed.setup_seeds([URL('http://example2.com/prefix2/')])
    assert not url_allowed.url_allowed(URL('http://example.com'))
    assert url_allowed.url_allowed(URL('http://www.example.com/prefix11'))
    assert not url_allowed.url_allowed(URL('http://example2.com'))
    assert not url_allowed.url_allowed(URL('http://www.example2.com/prefix21'))
    assert not url_allowed.url_allowed(URL('http://www.example2.com/prefix2'))
    assert url_allowed.url_allowed(URL('http://www.example2.com/prefix2/'))
    assert url_allowed.url_allowed(URL('http://www.example2.com/prefix2/foo'))

    url_allowed.setup(policy='OnlySeeds')
    url_allowed.setup_seeds([URL('http://example.com')])
    assert url_allowed.url_allowed(URL('http://example.com'))
    assert not url_allowed.url_allowed(URL('http://example.com/foo'))

    url_allowed.setup(policy='AllDomains')
    url_allowed.setup_seeds([URL('http://example.com')])
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


def test_setup_seeds_prefix():
    seeds = {'http://example.com/asdf', 'http://example.com/a', 'http://example.com/a',
             'http://example.com/b', 'http://example.com/asdff', 'http://example2.com/a'}
    url_allowed.setup(policy='SeedsPrefix')
    url_allowed.setup_seeds([URL(s) for s in seeds])

    SEEDS = {'example.com': {'/a', '/b'}, 'example2.com': {'/a'}}
    assert SEEDS == url_allowed.SEEDS
