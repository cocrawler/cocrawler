'''
URL class and transformations for the cocrawler.

We apply "safe" transformations early and often.

We use some "less safe" transformations when we are checking if
we've crawled (or may have crawled) an URL before. This helps
us keep out of some kinds of crawler traps, and can save us
a lot of effort overall.

TODO: SURT
'''

from collections import namedtuple

import urllib.parse
import logging

import tldextract

LOGGER = logging.getLogger(__name__)


def clean_webpage_links(link):
    '''
    Webpage links have lots of random crap in them that we'd like to
    clean up before calling urljoin() on them.

    Some of these come from
    https://github.com/django/django/blob/master/django/utils/http.py#L287
    and https://bugs.chromium.org/p/chromium/issues/detail?id=476478

    The following seem to be browser-universals?
    '''

    # remove leading and trailing white space, and unescaped control chars.
    # (this is safe even when we're looking at utf8 -- all utf8 bytes have high bit set)
    # (don't touch escaped chars)
    link = link.strip(' \x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f'
                      '\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f')

    if link.startswith('///'):
        # leaving extra slashes is pretty silly, so trim it down do a
        # same-host-absolute-path url. unsafe, but the url as-is is likely invalid anyway.
        link = '/' + link.lstrip('/')
        # note that this doesn't touch /// links with schemes or hostnames

    # TODO: do something here with mid-url unquoted control chars/spaces/utf-8? or do it elsewhere?

    return link


def special_seed_handling(url):
    '''
    We don't expect seed-lists to be very clean.
    '''
    url = clean_webpage_links(url)

    # use urlparse to accurately test if a scheme is present
    parts = urllib.parse.urlparse(url)
    if parts.scheme == '':
        if url.startswith('//'):
            url = 'http:' + url
        else:
            url = 'http://' + url
    return url

valid_hex = set('%02x' % i for i in range(256))
valid_hex.update(set('%02X' % i for i in range(256)))


def safe_url_canonicalization(url):
    '''
    Do everything to the url which shouldn't possibly hurt its semantics
    Good discussion: https://en.wikipedia.org/wiki/URL_normalization
    '''

    # capitalize quoted characters (XXX or should these be lowercased?)
    pieces = url.split('%')
    url = pieces.pop(0)
    for p in pieces:
        if len(p) > 1 and p[:2] in valid_hex:
            p = p[:2].upper() + p[2:]
        url += '%' + p

    (scheme, netloc, path, parms, query, fragment) = urllib.parse.urlparse(url)
    scheme = scheme.lower()
    netloc = netloc.lower()
    if path == '':
        path = '/'

    # TODO: punycode hostnames (codec='punycode')

    if scheme == 'http' and netloc[-3:] == ':80':
        netloc = netloc[:-3]
    if scheme == 'https' and netloc[-4:] == ':443':
        netloc = netloc[:-4]

    # TODO:
    #  decode unnecessary quoted bytes %41-%5A  %61-%7A %30-%39 %2D %2E %5F %7E
    #  encode necessary bytes -- need to take the str to bytes first -- different list for each part
    #
    #  decide what to do with urls containing invalid utf8, see
    #   https://github.com/internetarchive/surt/issues/19
    #   https://github.com/commoncrawl/ia-web-commons/issues/6
    #   preserve them exactly?

    if fragment is not '':
        fragment = '#' + fragment
    return urllib.parse.urlunparse((scheme, netloc, path, parms, query, None)), fragment


def upgrade_url_to_https(url):
    # TODO
    #  use browser HSTS list to upgrade to https:
    #   https://chromium.googlesource.com/chromium/src/net/+/master/http/transport_security_state_static.json
    #  use HTTPSEverwhere? would have to have a fallback if https failed / redir to http
    return


def special_redirect(url, next_url):
    '''
    Classifies some redirects that we wish to do special processing for
    '''
    if abs(len(url.url) - len(next_url.url)) > 5:  # 5 = 'www.' + 's'
        return None

    if url.url == next_url.url:
        return 'same'

    if not url.url.endswith('/') and url.url + '/' == next_url.url:
        return 'addslash'

    if url.url.endswith('/') and url.url == next_url.url + '/':
        return 'removeslash'

    if url.url.replace('http', 'https', 1) == next_url.url:
        return 'tohttps'
    if url.url.startswith('https') and url.url.replace('https', 'http', 1) == next_url.url:
        return 'tohttp'

    if url.urlparse.netloc.startswith('www.'):
        if url.url.replace('www.', '', 1) == next_url.url:
            return 'tononwww'
        else:
            if url.url.replace('www.', '', 1).replace('http', 'https', 1) == next_url.url:
                return 'tononwww+tohttps'
            elif (url.url.startswith('https') and
                  url.url.replace('www.', '', 1).replace('https', 'http', 1) == next_url.url):
                return 'tononwww+tohttp'
    elif next_url.urlparse.netloc.startswith('www.'):
        if url.url == next_url.url.replace('www.', '', 1):
            return 'towww'
        else:
            if next_url.url.replace('www.', '', 1) == url.url.replace('http', 'https', 1):
                return 'towww+tohttps'
            elif (url.url.startswith('https') and
                  next_url.url.replace('www.', '', 1) == url.url.replace('https', 'http', 1)):
                return 'towww+tohttp'

    return None


def get_domain(hostname):
    # TODO config option to set include_psl_private_domains=True ?
    #  sometimes we do want *.blogspot.com to all be different tlds
    #  right now set externally, see https://github.com/john-kurkowski/tldextract/issues/66
    #  the makefile for this repo sets it to private and there is a unit test for it
    return tldextract.extract(hostname).registered_domain


def get_hostname(url, parts=None, remove_www=False):
    # TODO: also duplicated in url_allowed.py
    # note www handling, different parts of the code
    # XXX www.com is a valid domain name. I need to be careful to not damage it.
    if not parts:
        parts = urllib.parse.urlparse(url)
    hostname = parts.netloc
    if remove_www and hostname.startswith('www.'):
        domain = get_domain(hostname)
        if not domain.startswith('www.'):
            hostname = hostname[4:]
    return hostname

# stolen from urllib/parse.py
ParseResult = namedtuple('ParseResult', 'scheme netloc path params query fragment')


class URL(object):
    '''
    Container for urls and url processing.
    Precomputes a lot of stuff upon creation, which is usually done in a burner thread.
    Currently idempotent.
    '''
    def __init__(self, url, urljoin=None, seed=False):
        if seed:
            url = special_seed_handling(url)
        url = clean_webpage_links(url)

        if urljoin:
            if isinstance(urljoin, str):
                urljoin = URL(urljoin)
            # optimize a few common cases to dodge full urljoin cost
            if url.startswith('http://') or url.startswith('https://'):
                pass
            elif url.startswith('/') and not url.startswith('//'):
                url = urljoin.urlparse.scheme + '://' + urljoin.hostname + url
            else:
                url = urllib.parse.urljoin(urljoin.url, url)  # expensive

        # XXX incorporate safe_url_canonicalization() with the below frag and / handling

        if '#' in url:
            # we have a frag. split on the leftmost #
            url, frag = url.split('#', 1)
            if frag == '':
                frag = None
            self._original_frag = frag
        else:
            self._original_frag = None

        try:
            self._urlparse = urllib.parse.urlparse(url)  # expensive
        except ValueError:
            LOGGER.info('invalid url %s sent into URL constructor', url)
            # TODO: my code assumes URL() returns something valid, so...
            raise

        if self._urlparse.path == '':
            # we want this to be '/' ... pretty much a design error in urllib, but I digress.
            urlp = list(self._urlparse)
            urlp[2] = '/'
            self._urlparse = ParseResult(*urlp)

        # url is currently decoded, and may or may not have parts of the hostname in punycode
        # XXX make sure it's valid encoding, canonicalize hostname to punycode ('xn--...') hint: (codec='punycode')

        self._url = urllib.parse.urlunparse(self._urlparse)  # final canonicalization

        self._netloc = self.urlparse.netloc
        self._hostname = get_hostname(None, parts=self._urlparse)

        # XXX todo use registered_domain to avoid harming domains like www.com
        self._hostname_without_www = get_hostname(None, parts=self._urlparse, remove_www=True)

        # tldextract basically has its own urlparse built-in :-/
        self._registered_domain = tldextract.extract(self._url).registered_domain

    @property
    def url(self):
        return self._url

    @property
    def urlparse(self):
        return self._urlparse

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
    def registered_domain(self):
        return self._registered_domain

    @property
    def original_frag(self):
        return self._original_frag
