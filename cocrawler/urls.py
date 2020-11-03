'''
URL class and transformations for the cocrawler.

We apply "safe" transformations early and often.

We apply "unsafe" transformations right after parsing an url out of
a webpage. (These represent things that browsers do but aren't
in the RFC, like discarding /r/n in the middle of hostnames.)

See cocrawler/data/html-parsin-test.html for an analysis of browser
transformations.




'''

from collections import namedtuple
import urllib.parse
import logging
import re
import html

import tldextract

from . import surt

LOGGER = logging.getLogger(__name__)


'''
Notes from reading RFC 3986:

General rule: always unquote A-Za-z0-9-._~  # these are never delims
  called 'unreserved' in the rfc ... x41-x5a x61-x7a x30-x39 x2d x2e x5f x7e

reserved:
 general delims :/?#[]@
 sub delims !$&'()*+,;=

scheme blah blah
netloc starts with //, ends with /?# and has internal delims of :@
    hostname can be ip4 literal or [ip4 or ip6 literal] so also dots (ipv4) and : (ipv6)
      (this is the only place where [] are allowed unquoted)
path
    a character in a path is unreserved %enc sub-delims :@ and / is the actual delimiter
      so, general-delims other than :/@ must be quoted & kept that way
      that means ?#[] need quoting
    . and .. are special (see section 5.2)
    sub-delims can be present and don't have to be quoted
query
    same as path chars but adds /? to chars allowed
     so #[] still need quoting
    we are going to split query up using &= which are allowed characters
fragment
    same chars as query

due to quoting, % must be quoted

'''


def is_absolute_url(url):
    if url[0:2] == '//':
        return True
    # TODO: allow more schemes
    if url[0:7].lower() == 'http://' or url[0:8].lower() == 'https://':
        return True
    return False


def clean_webpage_links(link, urljoin=None):
    '''
    Webpage links have lots of random crap in them, which browsers tolerate,
    that we'd like to clean up before calling urljoin() on them.

    Also, since cocrawler allows a variety of html parsers, it's likely that
    we will get improperly-terminated urls that result in the parser returning
    the rest of the webpage as an url, etc etc.

    Some of these come from
    https://github.com/django/django/blob/master/django/utils/http.py#L287
    and https://bugs.chromium.org/p/chromium/issues/detail?id=476478

    See manual tests in cocrawler/data/html-parsing-test.html

    TODO: headless browser testing to automate this
    '''

    # remove leading and trailing white space and unescaped control chars.
    link = link.strip('\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f'
                      '\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f ')

    # FF and Chrome interpret both ///example.com and http:///example.com as a hostname
    m = re.match(r'(?:https?:)?/{3,}', link, re.I)
    if m:
        start = m.group(0)
        link = start.rstrip('/') + '//' + link.replace(start, '', 1)
    # ditto for \\\ -- and go ahead and fix up http:\\ while we're here
    m = re.match(r'(?:https?:)?\\{2,}', link, re.I)
    if m:
        start = m.group(0)
        link = start.rstrip('\\') + '//' + link.replace(start, '', 1)

    # and the \ that might be after the hostname?
    if is_absolute_url(link):
        start = link.find('://') + 3  # works whether we have a scheme or not
        m = re.search(r'[\\/?#]', link[start:])
        if m:
            if m.group(0) == '\\':
                link = link[0:start] + link[start:].replace('\\', '/', 1)

    # the current standard requires one round of &ent; unescaping, with tolerance for naked &
    if '&' in link:
        link = html.unescape(link)

    '''
    Runaway urls

    We allow pluggable parsers, and some of them might non-clever and send us the entire
    rest of the document as an url... or it could be that the webpage lacks a closing
    quote for one of its urls, which can confuse diligent parsers.

    There are formal rules for this in html5, by testing I see that FF and Chrome both
    truncate *undelimited* urls at the first >\r\n

    We have no idea which urls were delimited or not at this point. So, only molest
    ones which seem awfully long.
    '''

    if len(link) > 300:  # arbitrary choice
        m = re.match(r'(.*?)[<>\"\'\r\n ]', link)  # rare  in urls and common in html markup
        if m:
            link = m.group(1)
        if len(link) > 2000:
            if link.startswith('javascript:') or link.startswith('data:'):
                return ''
            logstr = link[:50] + '...'
            LOGGER.info('webpage urljoin=%s has an invalid-looking link %s of length %d',
                        str(urljoin), logstr, len(link))
            return ''  # will urljoin to the urljoin

    # FF and Chrome eat ^I^J^M in the middle of quoted urls
    link = link.replace('\t', '')
    link = link.replace('\r', '')
    link = link.replace('\n', '')

    return link


def remove_dot_segments(path):
    '''
    Algorithm from RFC 3986. urllib.parse has this algorithm, but it's hidden in urljoin()
    This is a stand-alone version. Since this is working on a non-relative url, path MUST begin with '/'
    '''
    if path[0] != '/':
        # raise ValueError('Invalid path, must start with /: '+path)
        # lots of invalid webpages! examples; '&x39;/', '%20/'
        return path

    segments = path.split('/')
    # drop empty segment pieces to avoid // in output... but not the first segment
    segments[1:-1] = filter(None, segments[1:-1])
    resolved_segments = []
    for s in segments[1:]:
        if s == '..':
            try:
                resolved_segments.pop()
            except IndexError:
                # discard the .. if it's at the beginning
                pass
        elif s == '.':
            continue
        else:
            resolved_segments.append(s)
    return '/' + '/'.join(resolved_segments)


valid_hex = set('%02x' % i for i in range(256))
valid_hex.update(set('%02X' % i for i in range(256)))

unreserved = set('%02X' % i for i in range(0x41, 0x5b))  # A-Z
unreserved.update(set('%02X' % i for i in range(0x61, 0x7b)))  # a-z
unreserved.update(set('%02X' % i for i in range(0x30, 0x3a)))  # 0-9
unreserved.update(set(('2D', '2E', '5F', '7E')))  # -._~

subdelims = set(('21', '24', '3B', '3D'))  # !$;=
subdelims.update(set('%02X' % i for i in range(0x26, 0x2d)))  # &'()*+,

unquote_in_path = subdelims.copy()
unquote_in_path.update(set(('3A', '40')))  # ok: :@

quote_in_path = {' ': '%20'}

unquote_in_query = subdelims.copy()
unquote_in_query.update(set(('3A', '2F', '3F', '40')))  # ok: :/?@
unquote_in_query.remove('26')  # not ok: &=
unquote_in_query.remove('3D')

quote_in_query = {' ': '+'}

unquote_in_frag = unquote_in_query.copy()


def unquote(text, safe):
    pieces = text.split('%')
    text = pieces.pop(0)
    for p in pieces:
        if text.endswith('%'):  # deal with %%
            text += '%' + p
            continue
        quote = p[:2]
        rest = p[2:]
        if quote in valid_hex:
            quote = quote.upper()
        if quote in safe:
            text += chr(int(quote, base=16)) + rest
        else:
            text += '%' + quote + rest
    return text


def quote(text, quoteme):
    ret = ''
    for c in text:
        if c in quoteme:
            c = quoteme[c]
        ret += c
    return ret


def safe_url_canonicalization(url):
    '''
    Do everything to the url which should not change it
    Good discussion: https://en.wikipedia.org/wiki/URL_normalization
    '''

    original_url = url
    url = unquote(url, unreserved)

    try:
        (scheme, netloc, path, query, fragment) = urllib.parse.urlsplit(url)
    except ValueError:
        LOGGER.info('invalid url %s', url)
        return original_url, ''

    scheme = scheme.lower()
    if scheme not in ('http', 'https', 'ftp'):
        return original_url, ''

    netloc = surt.netloc_to_punycanon(scheme, netloc)

    if path == '':
        path = '/'
    try:
        path = remove_dot_segments(path)
    except ValueError:
        LOGGER.info('remove_dot_segments puking on url %s', url)
        raise
    path = path.replace('\\', '/')  # might not be 100% safe but is needed for Windows buffoons
    path = unquote(path, unquote_in_path)
    path = quote(path, quote_in_path)

    query = unquote(query, unquote_in_query)
    query = quote(query, quote_in_query)

    if fragment != '':
        fragment = '#' + unquote(fragment, unquote_in_frag)

    return urllib.parse.urlunsplit((scheme, netloc, path, query, None)), fragment


def upgrade_url_to_https(url):
    # TODO
    #  use browser HSTS list to upgrade to https:
    #   https://chromium.googlesource.com/chromium/src/net/+/master/http/transport_security_state_static.json
    #  use HTTPSEverwhere? would have to have a fallback if https failed / redir to http
    #   do not use "mixed" rules from this dataset
    #  .app tld is 100% HTTPS
    return


def special_redirect(url, next_url):
    '''
    Classifies some redirects that we wish to do special processing for

    # XXX note that we are not normalizing unicode other than the surt hostname
    '''

    if not isinstance(url, str):
        urlsplit = url.urlsplit
        url = url.url
    else:
        urlsplit = urllib.parse.urlsplit(url)
    if not isinstance(next_url, str):
        next_urlsplit = next_url.urlsplit
        next_url = next_url.url
    else:
        next_urlsplit = urllib.parse.urlsplit(next_url)

    if abs(len(url) - len(next_url)) > 5:  # 5 = 'www.' + 's'
        return None

    if url == next_url:
        return 'same'

    if url.casefold() == next_url.casefold():
        return 'case-change'

    if not url.endswith('/') and url + '/' == next_url:
        return 'addslash'

    if url.endswith('/') and url == next_url + '/':
        return 'removeslash'

    if url.replace('http', 'https', 1) == next_url:
        return 'tohttps'
    if url.startswith('https') and url.replace('https', 'http', 1) == next_url:
        return 'tohttp'

    if urlsplit.netloc.startswith('www.'):
        if url.replace('www.', '', 1) == next_url:
            return 'tononwww'
        else:
            if url.replace('www.', '', 1).replace('http', 'https', 1) == next_url:
                return 'tononwww+tohttps'
            elif (url.startswith('https') and
                  url.replace('www.', '', 1).replace('https', 'http', 1) == next_url):
                return 'tononwww+tohttp'
    elif next_urlsplit.netloc.startswith('www.'):
        if url == next_url.replace('www.', '', 1):
            return 'towww'
        else:
            if next_url.replace('www.', '', 1) == url.replace('http', 'https', 1):
                return 'towww+tohttps'
            elif (url.startswith('https') and
                  next_url.replace('www.', '', 1) == url.replace('https', 'http', 1)):
                return 'towww+tohttp'

    return None


def get_domain(hostname):
    # TODO config option to set include_psl_private_domains=False
    #  currently we force *.blogspot.com to all be different domains
    #  another call below in URL __init__
    try:
        tlde = tldextract.extract(hostname, include_psl_private_domains=True)
    except IndexError:
        # can be raised for punycoded hostnames
        raise
    rd = tlde.registered_domain
    if rd:
        return rd
    else:
        return tlde.suffix  # example used to be: s3.amazonaws.com, but no longer


def get_hostname(url, parts=None, remove_www=False):
    # TODO: also duplicated in url_allowed.py
    # XXX audit code for other places www is explicitly mentioned
    if not parts:
        parts = urllib.parse.urlsplit(url)
    _, _, hostname, _ = surt.parse_netloc(parts.netloc)
    if remove_www and hostname.startswith('www.'):
        domain = get_domain(hostname)
        if not domain.startswith('www.'):
            hostname = hostname[4:]
    return hostname


# stolen from urllib/parse.py
SplitResult = namedtuple('SplitResult', 'scheme netloc path query fragment')


class URL(object):
    '''
    Container for urls and url processing.
    Precomputes a lot of stuff upon creation, which is usually done in a burner thread.
    Currently idempotent.
    '''
    def __init__(self, url, urljoin=None, surt_strip_trailing_slash=False):
        url = clean_webpage_links(url, urljoin=urljoin)

        if urljoin:
            if isinstance(urljoin, str):
                urljoin = URL(urljoin)
            # optimize a few common cases to dodge full urljoin cost
            if url.startswith('http://') or url.startswith('https://'):
                pass
            elif url.startswith('/') and not url.startswith('//'):
                url = urljoin.urlsplit.scheme + '://' + urljoin.hostname + url
            else:
                url = urllib.parse.urljoin(urljoin.url, url)  # expensive

        # TODO safe_url_canon has the parsed url, have it pass back the parts
        url, frag = safe_url_canonicalization(url)

        if len(frag) > 0:
            self._original_frag = frag
        else:
            self._original_frag = None

        try:
            self._urlsplit = urllib.parse.urlsplit(url)  # expensive
        except ValueError:
            LOGGER.info('invalid url %s sent into URL constructor', url)
            # TODO: my code assumes URL() returns something valid, so...
            # attempt to get rid of anything that would cause an invalid ipv6 ValueError
            # XXX this isn't the only place that needs tweaking
            url = url.replace('[', '').replace(']', '')
            self._urlsplit = urllib.parse.urlsplit(url)

        (scheme, netloc, path, query, _) = self._urlsplit

        if path == '':
            path = '/'

        # TODO: there's a fair bit of duplicate computing in here
        netloc = surt.netloc_to_punycanon(scheme, netloc)
        self._netloc = netloc
        self._hostname = surt.hostname_to_punycanon(netloc)
        self._hostname_without_www = surt.discard_www_from_hostname(self._hostname)
        self._surt = surt.surt(url, surt_strip_trailing_slash=surt_strip_trailing_slash)

        self._urlsplit = SplitResult(scheme, netloc, path, query, '')
        self._url = urllib.parse.urlunsplit(self._urlsplit)  # final canonicalization
        try:
            # see note above about private domains
            self._tldextract = tldextract.extract(self._url, include_psl_private_domains=True)
        except IndexError:
            # can be raised for punycoded hostnames
            raise
        self._registered_domain = self._tldextract.registered_domain
        if not self._registered_domain:
            self._registered_domain = self._tldextract.suffix  # example used to be: s3.amazonaws.com, but no longer

    @property
    def url(self):
        return self._url

    def __str__(self):
        return self._url

    @property
    def urlsplit(self):
        return self._urlsplit

    @property
    def netloc(self):
        return self._netloc

    @property
    def hostname(self):
        return self._hostname

    @property
    def hostname_without_www(self):
        return self._hostname_without_www

    @property
    def surt(self):
        return self._surt

    @property
    def registered_domain(self):
        return self._registered_domain

    @property
    def original_frag(self):
        return self._original_frag
