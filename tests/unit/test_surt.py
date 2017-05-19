import pytest

import cocrawler.surt as surt


def test_parse_netloc():
    assert surt.parse_netloc('') == ('', '', '', '')
    assert surt.parse_netloc('cocrawl.com') == ('', '', 'cocrawl.com', '')
    assert surt.parse_netloc('cocrawl.com:443') == ('', '', 'cocrawl.com', '443')
    assert surt.parse_netloc('cocrawl.com:') == ('', '', 'cocrawl.com', '')
    assert surt.parse_netloc('@cocrawl.com') == ('', '', 'cocrawl.com', '')
    assert surt.parse_netloc(':@cocrawl.com:') == ('', '', 'cocrawl.com', '')
    assert surt.parse_netloc('foo@cocrawl.com') == ('foo', '', 'cocrawl.com', '')
    assert surt.parse_netloc('foo:@cocrawl.com') == ('foo', '', 'cocrawl.com', '')
    assert surt.parse_netloc(':bar@cocrawl.com') == ('', 'bar', 'cocrawl.com', '')
    assert surt.parse_netloc('foo:bar@cocrawl.com') == ('foo', 'bar', 'cocrawl.com', '')
    assert surt.parse_netloc('foo:bar@cocrawl.com:8080') == ('foo', 'bar', 'cocrawl.com', '8080')
    assert surt.parse_netloc('[foo:80') == ('', '', '[foo:80', '')  # intentional
    assert surt.parse_netloc('[foo]') == ('', '', '[foo]', '')
    assert surt.parse_netloc('[foo]:80') == ('', '', '[foo]', '80')
    assert surt.parse_netloc('[foo:foo]:80') == ('', '', '[foo:foo]', '80')
    assert surt.parse_netloc('@[foo:foo]:80') == ('', '', '[foo:foo]', '80')
    assert surt.parse_netloc(':@[foo:foo]:80') == ('', '', '[foo:foo]', '80')
    assert surt.parse_netloc('u:@[foo:foo]:80') == ('u', '', '[foo:foo]', '80')
    assert surt.parse_netloc(':p@[foo:foo]:80') == ('', 'p', '[foo:foo]', '80')
    assert surt.parse_netloc('u:p@[foo:foo]:80') == ('u', 'p', '[foo:foo]', '80')
    assert surt.parse_netloc(':@[foo:foo]') == ('', '', '[foo:foo]', '')
    assert surt.parse_netloc('u:@[foo:foo]') == ('u', '', '[foo:foo]', '')
    assert surt.parse_netloc(':p@[foo:foo]') == ('', 'p', '[foo:foo]', '')
    assert surt.parse_netloc('u:p@[foo:foo]') == ('u', 'p', '[foo:foo]', '')


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

    assert surt.hostname_to_punycanon('العربية.museum') == 'xn--mgbcd4a2b0d2b.museum'  # right-to-left
    assert surt.hostname_to_punycanon('עברית.museum') == 'xn--5dbqzzl.museum'  # right-to-left


@pytest.mark.xfail(reason='turkish lower-case FAIL')
def test_hostname_to_punycanon_turkish_tricky():
    # That's not a normal 'I', it's an upper-case I without a dot. For some Unicode reason
    # it's something that you can't just lowercase. Python does not make it easy to dtrt here.
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
    return
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
    assert surt.surt("http://archive.org/goo/") == 'org,archive)/goo'
    assert surt.surt("http://archive.org/goo/?") == 'org,archive)/goo'
    assert surt.surt("http://archive.org/goo/?b&a") == 'org,archive)/goo?a&b'
    assert surt.surt("http://archive.org/goo/?a=2&b&a=1") == 'org,archive)/goo?a=1&a=2&b'

    # trailing comma mode
    assert surt.surt("http://archive.org/goo/?a=2&b&a=1", trailing_comma=True) == 'org,archive,)/goo?a=1&a=2&b'
    assert surt.surt("dns:archive.org", trailing_comma=True) == 'dns:archive.org'
    assert surt.surt("warcinfo:foo.warc.gz", trailing_comma=True) == 'warcinfo:foo.warc.gz'

    # PHP session id:
    assert surt.surt("http://archive.org/index.php?PHPSESSID=0123456789abcdefghijklemopqrstuv&action=profile;u=4221") == 'org,archive)/index.php?action=profile;u=4221'

    # WHOIS url:
    assert surt.surt("whois://whois.isoc.org.il/shaveh.co.il") == 'il,org,isoc,whois)/shaveh.co.il'

    # Yahoo web bug. See https://github.com/internetarchive/surt/issues/1
    assert surt.surt('http://visit.webhosting.yahoo.com/visit.gif?&r=http%3A//web.archive.org/web/20090517140029/http%3A//anthonystewarthead.electric-chi.com/&b=Netscape%205.0%20%28Windows%3B%20en-US%29&s=1366x768&o=Win32&c=24&j=true&v=1.2') == 'com,yahoo,webhosting,visit)/visit.gif?&b=netscape%205.0%20(windows;%20en-us)&c=24&j=true&o=win32&r=http://web.archive.org/web/20090517140029/http://anthonystewarthead.electric-chi.com/&s=1366x768&v=1.2'
