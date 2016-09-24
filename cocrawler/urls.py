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
        parts = ('http',) + parts[1:]
        url = urllib.parse.urlunparse(parts)
    return url

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
        if len(p) > 1:
            p = p[:2].upper() + p[2:]
        url += '%' + p

    (scheme, netloc, path, parms, query, fragment) = urllib.parse.urlparse(url)
    scheme = scheme.lower()
    netloc = netloc.lower()
    if netloc[-3:] == ':80':
        netloc = netloc[:-3]

    # TODO:
    #  decode unnecessary quotes %41-%5A  %61-%7A %30-%39 %2D %2E %5F %7E
    #  encode necessary quotees -- need to take the str to bytes first -- different list for each part

    if fragment is not '':
        fragment = '#' + fragment
    return urllib.parse.urlunparse((scheme, netloc, path, parms, query, None)), fragment

def upgrade_url_to_https(url):
    # uses HSTS to upgrade to https:
    #  https://chromium.googlesource.com/chromium/src/net/+/master/http/transport_security_state_static.json
    # use HTTPSEverwhere? would have to have a fallback if https failed
    return
