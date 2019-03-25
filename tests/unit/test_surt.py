import pytest

import cocrawler.surt as surt


def netloc1(netloc, parse):
    test_parse = surt.parse_netloc(netloc)
    assert test_parse == parse


def netloc2(netloc, parse):
    '''
    Bi-directional test
    '''
    test_parse = surt.parse_netloc(netloc)
    assert test_parse == parse
    test_unparse = surt.unparse_netloc(*test_parse)
    assert test_unparse == netloc


def test_parse_netloc():
    netloc2('', ('', '', '', ''))
    netloc2('cocrawl.com', ('', '', 'cocrawl.com', ''))
    netloc2('cocrawl.com:443', ('', '', 'cocrawl.com', '443'))
    netloc1('cocrawl.com:', ('', '', 'cocrawl.com', ''))
    netloc1('@cocrawl.com', ('', '', 'cocrawl.com', ''))
    netloc1(':@cocrawl.com:', ('', '', 'cocrawl.com', ''))
    netloc1('foo@cocrawl.com', ('foo', '', 'cocrawl.com', ''))
    netloc2('foo:@cocrawl.com', ('foo', '', 'cocrawl.com', ''))
    netloc2(':bar@cocrawl.com', ('', 'bar', 'cocrawl.com', ''))
    netloc2('foo:bar@cocrawl.com', ('foo', 'bar', 'cocrawl.com', ''))
    netloc2('foo:bar@cocrawl.com:8080', ('foo', 'bar', 'cocrawl.com', '8080'))
    netloc2('[foo:80', ('', '', '[foo:80', ''))  # intentional
    netloc2('[foo]', ('', '', '[foo]', ''))
    netloc2('[foo]:80', ('', '', '[foo]', '80'))
    netloc2('[foo:foo]:80', ('', '', '[foo:foo]', '80'))
    netloc1('@[foo:foo]:80', ('', '', '[foo:foo]', '80'))
    netloc1(':@[foo:foo]:80', ('', '', '[foo:foo]', '80'))
    netloc2('u:@[foo:foo]:80', ('u', '', '[foo:foo]', '80'))
    netloc2(':p@[foo:foo]:80', ('', 'p', '[foo:foo]', '80'))
    netloc2('u:p@[foo:foo]:80', ('u', 'p', '[foo:foo]', '80'))
    netloc1(':@[foo:foo]', ('', '', '[foo:foo]', ''))
    netloc2('u:@[foo:foo]', ('u', '', '[foo:foo]', ''))
    netloc2(':p@[foo:foo]', ('', 'p', '[foo:foo]', ''))
    netloc2('u:p@[foo:foo]', ('u', 'p', '[foo:foo]', ''))


def test_discard_www_from_hostname():
    assert surt.discard_www_from_hostname('example.com') == 'example.com'
    assert surt.discard_www_from_hostname('ww.example.com') == 'ww.example.com'
    assert surt.discard_www_from_hostname('www99.example.com') == 'example.com'
    assert surt.discard_www_from_hostname('WWW99.example.com') == 'example.com'
    assert surt.discard_www_from_hostname('www99.www1.example.com') == 'www1.example.com'
    assert surt.discard_www_from_hostname('www1.com') == 'www1.com'
    assert surt.discard_www_from_hostname('www1www.example.com') == 'www1www.example.com'
    assert surt.discard_www_from_hostname('www999.com') == 'www999.com'
    assert surt.discard_www_from_hostname('www999.example.com') == 'www999.example.com'


def test_netloc_to_punycanon():
    assert surt.netloc_to_punycanon('http', 'Example.Com') == 'example.com'
    assert surt.netloc_to_punycanon('http', 'u:p@bücher.com:80') == 'u:p@xn--bcher-kva.com'
    assert surt.netloc_to_punycanon('http', 'u:p@bücher.com:8080') == 'u:p@xn--bcher-kva.com:8080'


def test_hostname_to_punycanon():
    assert surt.hostname_to_punycanon('bücher.com') == 'xn--bcher-kva.com'
    assert surt.hostname_to_punycanon('b\u00fccher.com') == 'xn--bcher-kva.com'  # same as ü
    assert surt.hostname_to_punycanon('b%C3%BCcher.com') == 'xn--bcher-kva.com'  # unicode bytes
    assert surt.hostname_to_punycanon('b%c3%bccher.com') == 'xn--bcher-kva.com'  # unicode bytes, lower-case %
    assert surt.hostname_to_punycanon('b%FCcher.com') == 'xn--bcher-kva.com'  # iso-8859-1 byte (latin-1)

    assert surt.hostname_to_punycanon('\u00C7.com') == 'xn--7ca.com'  # Ç.com
    assert surt.hostname_to_punycanon('\u0043\u0327.com') == 'xn--7ca.com'  # Ç written as combining characters

    assert surt.hostname_to_punycanon('b%C3%CC%FCcher.com') == 'xn--bcher-9qa7d0g.com'  # mixture result in latin-1 mangle

    assert surt.hostname_to_punycanon('日本語.jp') == 'xn--wgv71a119e.jp'  # CJKV example
    assert surt.hostname_to_punycanon('www.日本語.jp') == 'www.xn--wgv71a119e.jp'

    assert surt.hostname_to_punycanon('العربية.museum') == 'xn--mgbcd4a2b0d2b.museum'  # right-to-left
    assert surt.hostname_to_punycanon('עברית.museum') == 'xn--5dbqzzl.museum'  # right-to-left


@pytest.mark.xfail(reason='turkish lower-case FAIL', strict=True)
def test_hostname_to_punycanon_turkish_tricky():
    # This one is impossible because you have to know that this is Turkish before you can do
    # the Turkish-specific down case of dotless I (a normal I) to dotless i (ı)
    # https://www.w3.org/International/wiki/Case_folding#Turkish_i.2FI_etc.
    assert surt.hostname_to_punycanon('TÜRKİYE.com') == surt.hostname_to_punycanon('türkiye.com')
    assert surt.hostname_to_punycanon('TÜRKİYE.com') != surt.hostname_to_punycanon('türkıye.com')
    assert surt.hostname_to_punycanon('TÜRKIYE.com') != surt.hostname_to_punycanon('türkiye.com')
    assert surt.hostname_to_punycanon('TÜRKIYE.com') == surt.hostname_to_punycanon('türkıye.com')


def test_reverse_hostname_parts():
    assert surt.reverse_hostname_parts('example.com') == ['com', 'example']
    assert surt.reverse_hostname_parts('example.com') == ['com', 'example']
    assert surt.reverse_hostname_parts('foo.example.com') == ['com', 'example', 'foo']
    assert surt.reverse_hostname_parts('com') == ['com']
    assert surt.reverse_hostname_parts('[ipv6]') == ['[ipv6]']
    assert surt.reverse_hostname_parts('1.2.3.4') == ['1.2.3.4']


def test_surt():
    # tests drawn from github.com/internetarchive/surt (accessed may 18, 2017)

    assert surt.surt(None) == '-'
    assert surt.surt('') == '-'
    assert surt.surt("filedesc:foo.arc.gz") == 'filedesc:foo.arc.gz'
    assert surt.surt("filedesc:/foo.arc.gz") == 'filedesc:/foo.arc.gz'
    assert surt.surt("filedesc://foo.arc.gz") == 'filedesc://foo.arc.gz'
    assert surt.surt("warcinfo:foo.warc.gz") == 'warcinfo:foo.warc.gz'
    assert surt.surt("dns:alexa.com") == 'dns:alexa.com'
    assert surt.surt("dns:archive.org") == 'dns:archive.org'

    assert surt.surt("http://www.archive.org/") == 'org,archive)/'
    assert surt.surt("http://archive.org/") == 'org,archive)/'
    assert surt.surt("http://archive.org/goo/") == 'org,archive)/goo/'
    assert surt.surt("http://archive.org/goo/?") == 'org,archive)/goo/'
    assert surt.surt("http://archive.org/goo/?b&a") == 'org,archive)/goo/?a&b'
    assert surt.surt("http://archive.org/goo/?a=2&b&a=1") == 'org,archive)/goo/?a=1&a=2&b'

    assert surt.surt("http://www.archive.org/", surt_strip_trailing_slash=True) == 'org,archive)'
    assert surt.surt("http://archive.org/goo/", surt_strip_trailing_slash=True) == 'org,archive)/goo'
    assert surt.surt("http://archive.org/goo/?", surt_strip_trailing_slash=True) == 'org,archive)/goo'
    assert surt.surt("http://archive.org/goo/?b&a", surt_strip_trailing_slash=True) == 'org,archive)/goo?a&b'

    # trailing comma mode
    #assert surt.surt("http://archive.org/goo/?a=2&b&a=1", trailing_comma=True) == 'org,archive,)/goo?a=1&a=2&b'
    #assert surt.surt("dns:archive.org", trailing_comma=True) == 'dns:archive.org'
    #assert surt.surt("warcinfo:foo.warc.gz", trailing_comma=True) == 'warcinfo:foo.warc.gz'
    # PHP session id:
    #assert surt.surt("http://archive.org/index.php?PHPSESSID=0123456789abcdefghijklemopqrstuv&action=profile;u=4221") == 'org,archive)/index.php?action=profile;u=4221'
    # WHOIS url:
    #assert surt.surt("whois://whois.isoc.org.il/shaveh.co.il") == 'il,org,isoc,whois)/shaveh.co.il'

    # Yahoo web bug. See https://github.com/internetarchive/surt/issues/1
    assert surt.surt("http://example.com/city-of-M%C3%BCnchen.html") == 'com,example)/city-of-m%c3%bcnchen.html'

    # with my preferred downcasing strategy - eh screw it
    #assert surt.surt('http://visit.webhosting.yahoo.com/visit.gif?&r=http%3A//web.archive.org/web/20090517140029/http%3A//anthonystewarthead.electric-chi.com/&b=Netscape%205.0%20%28Windows%3B%20en-US%29&s=1366x768&o=Win32&c=24&j=true&v=1.2') == 'com,yahoo,webhosting,visit)/visit.gif?&b=Netscape%205.0%20(Windows;%20en-US)&c=24&j=true&o=@in32&r=http://web.archive.org/web/20090517140029/http://anthonystewarthead.electric-chi.com/&s=1366x768&v=1.2'

    # end of tests drawn from github.com/internetarchive/surt

    # from Sebastian Nagel of Common Crawl, but with my preferred utf8 policy
    # not yet implemented: normalization of latin-1,utf-8 in the path
    #assert surt.surt("http://example.com/city-of-M%FCnchen.html") == 'com,example)/city-of-m%c3%bcnchen.html'

    # and unique to CoCrawler (so far)
    assert surt.surt("http://Example.Com/Goo/") == 'com,example)/goo/'
    assert surt.surt("http://Example.Com:4445/Goo/") == 'com,example,:4445)/goo/'
    assert surt.surt("http://bücher.Com/Goo/") == 'com,xn--bcher-kva)/goo/'
    assert surt.surt("http://example.com/goo/;FOO=bar") == 'com,example)/goo/'
    assert surt.surt("http://example.com/goo/;FOO=bar?a=1&A=1&a=2") == 'com,example)/goo/?A=1&a=1&a=2'
    assert surt.surt("http://example.com/goo/%3bFOO=bar") == 'com,example)/goo/%3bfoo=bar'
    assert surt.surt("http://example.com/goo/%3bFOO=bar?a=1&A=1&a=2") == 'com,example)/goo/%3bfoo=bar?A=1&a=1&a=2'
