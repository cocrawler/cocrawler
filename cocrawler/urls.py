import urllib.parse
import tldextract

'''
URL transformations for the cocrawler.

We apply "safe" transformations early and often.

We use some "less safe" transformations when we are checking if
we've crawled (or may have crawled) an URL before. This helps
us keep out of some kinds of crawler traps, and can save us
a lot of effort overall.
'''

import urllib.parse

def clean_webpage_links(link):
    '''
    Webpages have lots of random crap in them that we'd like to
    clean up before calling urljoin() on them.

    Some of these come from
    https://github.com/django/django/blob/master/django/utils/http.py#L287
    and https://bugs.chromium.org/p/chromium/issues/detail?id=476478

    The following seem to be browser-universals?
    '''

    # leading and trailing white space, and unescaped control chars.
    # escaped chars are a different issue.
    link = link.strip(' \x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f'
                      '\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f')

    if link.startswith('///'):
        # leaving extra slashes is pretty silly, so trim it down do a
        # same-host-absolute-path url. unsafe.
        link = '/' + link.lstrip('/')

    # XXX do something here with mid-url control chars/spaces/utf-8? or do it elsewhere?

    return link

def special_seed_handling(url):
    '''
    We don't expect seed-lists to be very clean.
    '''
    url = clean_webpage_links(url)
    parts = urllib.parse.urlparse(url)
    if parts.scheme == '':
        # without a scheme, the parse was invalid. start over.
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
    if scheme == 'http' and netloc[-3:] == ':80':
        netloc = netloc[:-3]
    if scheme == 'https' and netloc[-4:] == ':443':
        netloc = netloc[:-4]

    # TODO:
    #  decode unnecessary quotes %41-%5A  %61-%7A %30-%39 %2D %2E %5F %7E
    #  encode necessary quotes -- need to take the str to bytes first -- different list for each part
    #  punycode hostnames

    if fragment is not '':
        fragment = '#' + fragment
    return urllib.parse.urlunparse((scheme, netloc, path, parms, query, None)), fragment

def upgrade_url_to_https(url):
    # uses HSTS to upgrade to https:
    #  https://chromium.googlesource.com/chromium/src/net/+/master/http/transport_security_state_static.json
    # use HTTPSEverwhere? would have to have a fallback if https failed
    return

def special_redirect(url, next_url, parts=None):
    '''
    Detect redirects like www to non-www, http to https, etc.
    '''
    # TODO get next url parts from caller

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
    # XXX config option to set include_psl_private_domains=True ?
    #  sometimes we do want *.blogspot.com to all be different tlds
    tlde = tldextract.extract(hostname)
    mylist = list(tlde) # make it easy to change
    if mylist[1] == 'www':
        mylist[1] = ''
    if mylist[2] == 'www':
        mylist[2] = ''
    return '.'.join(part for part in mylist[1:3] if part)

def get_hostname(url, parts=None, remove_www=False):
    # TODO: also duplicated in cocrawler.urls.
    # note www handling, different parts of the code 
    if not parts:
        parts = urllib.parse.urlparse(url)
    hostname = parts.netloc
    if remove_www and hostname.startswith('www.'):
        hostname = hostname[4:]
    return hostname

