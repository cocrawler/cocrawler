'''
Code related to generating webpage facets.

Two storage sizes:
   big -- all headers
   small -- summary only
Two speeds:
   fast - avoid expensive greps and parsing
   slow - grep everything, use expensive parsing if needed

TODO: find rss feeds (both link alternate and plain href to .xml or maybe .rss)
TODO: probe with DNT:1 and see who replies TK: N
'''

import re
import logging
from collections.abc import Mapping

from bs4 import BeautifulSoup

from . import stats

LOGGER = logging.getLogger(__name__)

save_x_headers = set(('x-powered-by', 'cf-ray', 'x-generator'))

special_image = set(('og:image', 'twitter:image'))


meta_name_content = set(('twitter:site', 'twitter:site:id', 'twitter:creator', 'twitter:creator:id',
                         'robots', 'charset', 'referrer', 'format-detection', 'generator',
                         'parsely-title', 'apple-itunes-app', 'google-play-app'))
meta_name_generator_special = ('wordpress', 'movable type', 'drupal')
meta_name_prefix = (('twitter:', 'twitter card'),)

meta_property_content = set(('twitter:site', 'twitter:site:id', 'twitter:creator', 'twitter:creator:id',
                             'fb:app_id', 'fb:admins'))
meta_property_prefix = (('al:', 'applinks'),
                        ('og:', 'opengraph'),
                        ('article:', 'opengraph'),
                        ('op:', 'fb instant'),
                        ('bt:', 'boomtrain'),)

link_rel = set(('canonical', 'alternate', 'amphtml', 'opengraph', 'origin',
                'next', 'prev', 'previous', 'me', 'author', 'shortlink'))

save_response_headers = ('refresh', 'server', 'set-cookie', 'strict-transport-security', 'tk')


def compute_all(html, head, body, headers, links, embeds, head_soup=None, url=None, condense=False, expensive=False):
    expensive = True  # XXX

    fhf = find_head_facets(head, head_soup, url=url)
    fgh = facets_grep(head, url=url)
    if expensive:
        fgb = facets_grep(body, url=url)
        #compare_head_body_grep(fgh, fgb, url)  # ~ 10% of the facets discovered in the body are also in the head
    else:
        fgb = []
    frh = facets_from_response_headers(headers)
    fe = facets_from_embeds(embeds)

    facets = [*fhf, *fgh, *fgb, *frh, *fe]

    for l in links:
        facets.append(('link', fixup_link_object(l)))
    for e in embeds:
        facets.append(('embed', fixup_link_object(e)))

    return facet_dedup(facets)


def fixup_link_object(obj):
    ret = obj.copy()
    if 'href' in ret:
        ret['href'] = ret['href'].url
    if 'src' in ret:
        ret['src'] = ret['src'].url
    return ret


def find_head_facets(head, head_soup, url=None):
    facets = []

    html = head_soup.find('html')
    if html:
        if html.get('lang'):
            facets.append(('html lang', html.get('lang')))
        if html.get('xml:lang'):
            facets.append(('html xml:lang', html.get('xml:lang')))

    base = head_soup.find('base')
    if base:
        if base.get('href'):
            facets.append(('base', base.get('href')))
            # can also have target= but we don't care

    meta = head_soup.find_all('meta', attrs={'name': True})  # 'name' collides, so use dict
    for m in meta:
        n = m.get('name').lower()
        content = m.get('content')
        if n and content:
            if len(content) > 100:
                content = content[:100]
            facets.append(('meta-name-'+n, content))  # XXX get all of these for now
            #if n in meta_name_content:
            #    facets.append(('meta-name-'+n, content)
            #if n == 'generator':
            #    cl = content.lower()
            #    for s in meta_name_generator_special:
            #        if s in cl:
            #            facets.append((s, True))
            #for pre in meta_name_prefix:
            #    prefix, title = pre
            #    if n.startswith(prefix):
            #        facets.append((title, True))

    meta = head_soup.find_all('meta', property=True)
    for m in meta:
        p = m.get('property').lower()
        content = m.get('content')
        if p and content:
            if len(content) > 100:
                content = content[:100]
            facets.append(('meta-property-'+p, content))  # XXX get all of these for now
            #if p in meta_property_content:
            #    facets.append((p, content))
            #for pre in meta_property_prefix:
            #    prefix, title = pre
            #    if p.startswith(prefix):
            #        facets.append((title, True))

    meta = head_soup.find_all('meta', attrs={'http-equiv': True})  # has a dash, so use dict
    for m in meta:
        p = m.get('http-equiv').lower()
        content = m.get('content')
        if p and content:
            if len(content) > 100:
                content = content[:100]
            extra = ''
            if p == 'refresh':
                for enclosing in m.parents:
                    if enclosing and enclosing.name == 'noscript':
                        extra = '-noscript'
                        break
            facets.append(('meta-http-equiv-'+p+extra, content))  # XXX get all of these for now... robots, refresh etc

    # link rel is muli-valued attribute, hence, a list
    linkrel = head_soup.find_all('link', rel=True)
    for l in linkrel:
        for rel in l.get('rel'):
            r = rel.lower()
            if r in link_rel:
                things = {}
                for thing in ('href', 'type', 'title', 'hreflang'):
                    t = l.get(thing)
                    if t is not None:
                        things[thing] = t
                facets.append(('link-rel-'+r, things))

    count = len(head_soup.find_all(integrity=True))
    if count:
        facets.append(('thing-script integrity', count))

    return facets


def facet_dedup(facets):
    '''
    Remove duplicate ('foo', True) facets. Keep all the ones with other values.
    '''
    if not facets:
        return []

    dups = set()
    ret = []
    for f in facets:
        a, b = f
        if b is True:
            if a not in dups:
                ret.append((a, b))
                dups.add(a)
        else:
            ret.append((a, b))
    return ret


def facets_grep(html, url=None):
    facets = []

    # if present, it's embedded in a <script> jsonl in the head or body
    if 'http://schema.org' in html or 'https://schema.org' in html:
        facets.append(('thing-schema.org', True))

    # this can be in js or a cgi arg
    if 'pub-' in html:
        pub_matches = re.findall(r'\bpub-\d{16}\b', html)
        if pub_matches:
            for p in pub_matches:
                facets.append(('thing-google publisher id', p))
        else:
            LOGGER.info('url %s had false positive for pub- facet', url.url)

    # this can be in js or a cgi arg
    if 'UA-' in html:
        ga_matches = re.findall(r'\bUA-\d{6,9}-\d{1,3}\b', html)
        if ga_matches:
            for g in ga_matches:
                facets.append(('thing-google analytics', g))
        else:
            # frequent false positive for meta http-equiv X-UA-Compatible, alas
            pass

    # js or id= cgi arg
    if 'GTM-' in html:
        gtm_matches = re.findall(r'\bGTM-[A-Z0-9]{4,7}\b', html)
        if gtm_matches:
            for g in gtm_matches:
                facets.append(('thing-google tag manager', g))
        else:
            LOGGER.info('url %s had false positive for GTM- facet', url.url)

    # standard FB code has both this and embed facebook.com/tr?id=... also a CSP
    if 'fbq(' in html:
        fbid_matches = re.findall(r'fbq\( \s? [\'"] init [\'"] , \s? [\'"] (\d{15,16}) [\'"]', html, re.X)
        if fbid_matches:
            for g in fbid_matches:
                facets.append(('thing-facebook events', g))
        else:
            LOGGER.info('url %s had false positive for facebook events facet', url.url)

    return facets


def clean_utf8(s):
    '''
    aiohttp uses surrogatescape all over the header processing.
    try to stop it from leaking any farther.
    '''
    try:
        s.encode()
    except UnicodeEncodeError:
        s = s.encode('utf-8', 'replace').decode()
    return s


def facets_from_response_headers(headers):
    if isinstance(headers, Mapping):
        headers_list = [[k.lower(), v] for k, v in headers.items()]
    else:
        headers_list = headers
    facets = []
    for h in headers_list:
        k, v = h
        v = clean_utf8(v)
        facets.append(('header-'+k, v))

    return facets


def facets_from_embeds(embeds):
    facets = []
    for link_object in embeds:  # this is both href and src embeds, but whatever
        url = link_object.get('href') or link_object.get('src')
        if not url:
            continue
        u = url.url
        if 'cdn.ampproject.org' in u:
            facets.append(('thing-google amp', True))
        if 'www.google-analytics.com' in u:
            # rare that it's used this way
            # XXX parse the publisher id out of the cgi
            facets.append(('thing-google analytics link', True))
        if 'googlesyndication.com' in u:
            facets.append(('thing-google adsense', True))
        if 'google.com/adsense/domains' in u:
            facets.append(('thing-google adsense for domains', True))
        if 'googletagmanager.com' in u:
            cgi = url.urlsplit.query
            cgi_list = cgi.split('&')
            for c in cgi_list:
                if c.startswith('id=GTM-'):
                    facets.append(('thing-google tag manager', c[3:]))
        if 'https://www.facebook.com/tr?' in u:  # img src
            cgi = url.urlsplit.query
            cgi_list = cgi.split('&')
            for c in cgi_list:
                if c.startswith('id='):
                    facets.append(('thing-facebook events', c[3:]))

    return facets


def compare_head_body_grep(fh, fb, url):
    '''
    We only occasionally run body greps, and there are unique ids
    that only appear in the body.
    '''
    head = set(fh)
    body = set(fb)
    for kv in body:
        k, v = kv
        if kv not in head:
            LOGGER.info('body grep discovered %s %s in url %s', k, v, url.url)
        else:
            LOGGER.info('both head and body grep discovered %s %s in url %s', k, v, url.url)


def condense_facets(facets):
    # turn foo:bar into foo:<count>
    #   meta name, meta property
    # ditch traditionally long things: meta-name-{description,keywords}
    count_colons('meta-property-', facets)
    count_colons('meta-name-', facets)

    return
