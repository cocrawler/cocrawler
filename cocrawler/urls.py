import urllib.parse
import tldextract

'''
URL class and transformations for the cocrawler.

We apply "safe" transformations early and often.

We use some "less safe" transformations when we are checking if
we've crawled (or may have crawled) an URL before. This helps
us keep out of some kinds of crawler traps, and can save us
a lot of effort overall.

TODO: SURT
'''

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
    # escaped chars are a different issue.
    link = link.strip(' \x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f'
                      '\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f')

    if link.startswith('///'):
        # leaving extra slashes is pretty silly, so trim it down do a
        # same-host-absolute-path url. unsafe.
        link = '/' + link.lstrip('/')

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

    # capitalize quotes, without looking at them very carefully
    # note that this capitalizes invalid things like %0g
    pieces = url.split('%')
    url = pieces.pop(0)
    for p in pieces:
        if len(p) > 1 and p[:2] in valid_hex:
            p = p[:2].upper() + p[2:]
        url += '%' + p

    (scheme, netloc, path, parms, query, fragment) = urllib.parse.urlparse(url)
    scheme = scheme.lower()
    netloc = netloc.lower()
    # TODO: punycode hostnames (codec='punycode')
    if scheme == 'http' and netloc[-3:] == ':80':
        netloc = netloc[:-3]
    if scheme == 'https' and netloc[-4:] == ':443':
        netloc = netloc[:-4]

    # TODO:
    #  decode unnecessary quoted bytes %41-%5A  %61-%7A %30-%39 %2D %2E %5F %7E
    #  encode necessary bytes -- need to take the str to bytes first -- different list for each part

    if fragment is not '':
        fragment = '#' + fragment
    return urllib.parse.urlunparse((scheme, netloc, path, parms, query, None)), fragment

def upgrade_url_to_https(url):
    # TODO
    #  use browser HSTS list to upgrade to https:
    #   https://chromium.googlesource.com/chromium/src/net/+/master/http/transport_security_state_static.json
    #  use HTTPSEverwhere? would have to have a fallback if https failed / redir to http
    return

# XXX switch to using url objects
def special_redirect(url, next_url, parts=None):
    '''
    Detect redirects like www to non-www, http to https
    '''
    if abs(len(url) - len(next_url)) > 5: # 5 = 'www.' + 's'
        return None

    if url == next_url:
        return 'same'

    if url.replace('http', 'https', 1) == next_url:
        return 'tohttps'
    if url.startswith('https') and url.replace('https', 'http', 1) == next_url:
        return 'tohttp'

    next_parts = urllib.parse.urlparse(next_url)
    if not parts:
        parts = urllib.parse.urlparse(url)

    if parts.netloc.startswith('www.'):
        if url.replace('www.', '', 1) == next_url:
            return 'tononwww'
        else:
            if url.replace('www.', '', 1).replace('http', 'https', 1) == next_url:
                return 'tononwww+tohttps'
            elif url.startswith('https') and url.replace('www.', '', 1).replace('https', 'http', 1) == next_url:
                return 'tononwww+tohttp'
    elif next_parts.netloc.startswith('www.'):
        if url == next_url.replace('www.', '', 1):
            return 'towww'
        else:
            if next_url.replace('www.', '', 1) == url.replace('http', 'https', 1):
                return 'towww+tohttps'
            elif url.startswith('https') and next_url.replace('www.', '', 1)== url.replace('https', 'http', 1):
                return 'towww+tohttp'

    return None

def get_domain(hostname):
    # TODOconfig option to set include_psl_private_domains=True ?
    #  sometimes we do want *.blogspot.com to all be different tlds
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

class URL(object):
    '''
    Container for urls and url processing.
    Precomputes a lot of stuff upon creation, which is usually done in a burner thread.
    Currently idempotent.

    TODO: examine common search check_encoding
    TODO: handle idna xn-- encoding consistently
    TODO: split out urljoin below into a method
    TODO: do something with frag

    '''
    def __init__(self, url, urljoin=None, seed=False):
        if seed:
            url = special_seed_handling(url)
        url = clean_webpage_links(url)
        if urljoin:
            # XXX do I need to canonicalize urljoin, just to be safe?
            if url.startswith('http://') or url.startswith('https://'):
                pass
            elif url.startswith('/') and not url.startswith('//'):
                url = get_hostname(urljoin) + url
            else:
                url = urllib.urljoin(url, urljoin) # exensive

        # XXX do an urljoin here for more canonicalization: trailing ?, / after hostname, etc
        self._url = url

        self._urlparse = urllib.parse.urlparse(url) # expensive
        self._netloc = self.urlparse.netloc
        self._hostname = get_hostname(None, parts=self._urlparse)
        self._hostname_without_www = get_hostname(None, parts=self._urlparse, remove_www=True)

        # tldextract basically has its own urlparse built-in :-/
        self._registered_domain = tldextract.extract(url).registered_domain

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

