'''
Code related to generating webpage facets.
'''

import re

from bs4 import BeautifulSoup

import stats

get_name_content = set(('twitter:site', 'twitter:site:id', 'twitter:creator', 'twitter:creator:id',
                        'robots', 'charset', 'http-equiv', 'referrer', 'format-detection', 'generator',
                        'parsely-title'))
get_name_generator_special = ('wordpress', 'movable type', 'drupal')
get_name_prefix = (('twitter:', 'twitter card'),)

get_property_content = set(('twitter:site', 'twitter:site:id', 'twitter:creator', 'twitter:creator:id',
                            'fb:app_id', 'fb:admins'))
get_property_prefix = (('al:', 'applinks'),
                       ('og:', 'opengraph'),
                       ('op:', 'fb instant'))

get_link_rel = set(('canonical', 'alternate', 'amphtml', 'opengraph', 'origin'))


def find_head_facets(head, url=None):
    '''
    We use html parsing, because the head is smallish and friends don't let
    friends parse html with regexes.

    beautiful soup + lxml2 parses only about 4-16 MB/s
    '''
    facets = []

    stats.stats_sum('beautiful soup head bytes', len(head))
    with stats.record_burn('beautiful soup head', url=url):
        soup = BeautifulSoup(head, 'html.parser')

    html = soup.find('html')
    if html:
        if html.get('lang'):
            facets.append(('html lang', html.get('lang')))
        if html.get('xml:lang'):
            facets.append(('html xml:lang', html.get('xml:lang')))

    base = soup.find('base')
    if base:
        if base.get('href'):
            facets.append(('base', base.get('href')))

    meta = soup.find_all('meta', attrs={'name': True})  # 'name' collides, so use dict
    for m in meta:
        n = m.get('name').lower()
        if n in get_name_content:
            facets.append((n, m.get('content')))
        if n == 'generator':
            g = m.get('content', '')
            gl = g.lower()
            for s in get_name_generator_special:
                if s in gl:
                    facets.append((s, True))
        for pre in get_name_prefix:
            prefix, title = pre
            if n.startswith(prefix):
                facets.append((title, True))

    meta = soup.find_all('meta', property=True)
    for m in meta:
        p = m.get('property').lower()
        if p in get_property_content:
            facets.append((p, m.get('content')))
        for pre in get_property_prefix:
            prefix, title = pre
            if p.startswith(prefix):
                facets.append((title, True))

    # link rel is muli-valued attribute, hence, a list
    linkrel = soup.find_all('link', rel=True)
    for l in linkrel:
        for rel in l.get('rel'):
            r = rel.lower()
            if r in get_link_rel:
                facets.append((r, (l.get('href', 'nohref'), l.get('type', 'notype'))))

    count = len(soup.find_all(integrity=True))
    if count:
        facets.append(('script integrity', count))

    return facets


def facet_dedup(facets):
    '''
    Remove duplicate ('foo', True) facets. Keep all the ones with other values.
    '''
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


def facets_grep(head, facets):
    # look for this one as a grep, because if present, it's embedded in a <script> jsonl
    if 'http://schema.org' in head:
        facets.append(('schema.org', True))

    pub_matches = re.findall(r'[\'"]pub-\d{15,18}[\'"]', head)  # actually 16 digits
    if pub_matches:
        for p in pub_matches:
            facets.append(('google publisher id', p.strip('\'"')))

    ga_matches = re.findall(r'[\'"]UA-\d{7,9}-\d{1,3}[\'"]', head)
    if ga_matches:
        for g in ga_matches:
            facets.append(('google analytics', g.strip('\'"')))

    return facets

save_response_headers = ('Refresh', 'Server', 'Set-Cookie', 'Strict-Transport-Security', 'X-Powered-By')


# XXX unused
def facets_from_response_headers(headers, facets):
    '''
    Refresh: N; url=http://...
    Server: ...
    Set-Cookie: ... (note Secure or HttpOnly) (note Secure sent over HTTP)
    Strict-Transport-Security:
    X-Powered-By:
    '''
    for rh in save_response_headers:
        if rh in headers:
            facets.append((rh, headers.get(rh)))

    return facets


# XXX not used, should be generalized using lists from adblockers
def facets_from_embeds(embeds, facets):
    for url in embeds:  # this is both href and src embeds, but whatever
        u = url.url
        if 'cdn.ampproject.org' in u:
            facets.append(('google amp', True))
        if 'www.google-analytics.com' in u:
            # frequently the above link doesn't actually appear as a link, it's hidden in the js snippet
            # so the U-NNNNN-N string detection code is better
            facets.append(('google analytics', True))
        if 'googlesyndication.com' in u:
            facets.append(('google adsense', True))
        if 'google.com/adsense/domains' in u:
            facets.append(('google adsense for domains', True))
        '''
        TODO: Google tag manager <iframe src="https://www.googletagmanager.com/ns.html?id=GTM-M9L9Q5
        also has a js version
        '''

    return facets
