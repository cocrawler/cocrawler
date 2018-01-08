'''
Code related to generating webpage facets.

For normal crawling, we only parse facets we think might be useful
for crawling and ranking: STS, twitter cards, facebook opengraph.

TODO: find rss feeds (both link alternate and plain href to .xml or maybe .rss)
TODO: probe with DNT:1 and see who replies TK: N

This module also contains code to post-facto process headers to
figure out what technologies are used in a website.

'''

import re

from bs4 import BeautifulSoup

from . import stats

meta_name_content = set(('twitter:site', 'twitter:site:id', 'twitter:creator', 'twitter:creator:id',
                         'robots', 'charset', 'http-equiv', 'referrer', 'format-detection', 'generator',
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

link_rel = set(('canonical', 'alternate', 'amphtml', 'opengraph', 'origin'))

save_response_headers = ('refresh', 'server', 'set-cookie', 'strict-transport-security', 'tk')


def compute_all(html, head, headers_list, embeds, url=None):
    facets = []
    facets.extend(find_head_facets(head, url=url))
    facets.extend(facets_grep(head))
    facets.extend(facets_from_response_headers(headers_list))
    facets.extend(facets_from_embeds(embeds))

    return facet_dedup(facets)


def find_head_facets(head, head_soup=None, url=None):
    '''
    We use html parsing, because the head is smallish and friends don't let
    friends parse html with regexes.

    beautiful soup + lxml2 parses only about 4-16 MB/s
    '''
    facets = []

    if head_soup is None:
        stats.stats_sum('beautiful soup head bytes', len(head))
        with stats.record_burn('beautiful soup head', url=url):
            try:
                soup = BeautifulSoup(head, 'lxml')
            except Exception as e:
                facets.append(('BeautifulSoupException', repr(e)))
                return facets

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
            # can also have target= but we don't care

    meta = soup.find_all('meta', attrs={'name': True})  # 'name' collides, so use dict
    for m in meta:
        n = m.get('name').lower()
        content = m.get('content')
        #if n in meta_name_content:
        #    facets.append((n, content)
        facets.append(('meta-name-'+n, content))  # XXX get all of these for now
        if n == 'generator':
            cl = content.lower()
            for s in meta_name_generator_special:
                if s in cl:
                    facets.append((s, True))
        for pre in meta_name_prefix:
            prefix, title = pre
            if n.startswith(prefix):
                facets.append((title, True))
        # XXX remember the ones we didn't save

    meta = soup.find_all('meta', property=True)
    for m in meta:
        p = m.get('property').lower()
        content = m.get('content')
        facets.append(('meta-property-'+p, content))  # XXX get all of these for now
        if p in meta_property_content:
            facets.append((p, content))
        for pre in meta_property_prefix:
            prefix, title = pre
            if p.startswith(prefix):
                facets.append((title, True))
        # XXX remember the ones we didn't save

    # link rel is muli-valued attribute, hence, a list
    linkrel = soup.find_all('link', rel=True)
    for l in linkrel:
        for rel in l.get('rel'):
            r = rel.lower()
            if r in link_rel:
                # type is useful if it's something like canonical + type=rss
                facets.append(('link-rel-'+r, (l.get('href', 'nohref'), l.get('type', 'notype'))))
            else:
                # XXX remember the ones we didn't save
                pass

    count = len(soup.find_all(integrity=True))
    if count:
        facets.append(('script integrity', count))

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


def facets_grep(head):
    facets = []
    # look for this one as a grep, because if present, it's embedded in a <script> jsonl
    if 'http://schema.org' in head or 'https://schema.org' in head:
        facets.append(('schema.org', True))

    # this can be in js or a cgi arg
    pub_matches = re.findall(r'[\'"\-=]pub-\d{16}[\'"&]', head)
    if pub_matches:
        for p in pub_matches:
            facets.append(('google publisher id', p.strip('\'"-=&')))

    # this can be in js or a cgi arg
    ga_matches = re.findall(r'[\'"\-=]UA-\d{7,9}-\d{1,3}[\'"&]', head)
    if ga_matches:
        for g in ga_matches:
            facets.append(('google analytics', g.strip('\'"-=&')))

    # GTM-[A-Z0-9]{4,6} -- script text or id= cgi arg

    # noscript: img src=https://www.facebook.com/tr?id=\d{16}&
    # script: fbq('init', '\d{16}', and https://connect.facebook.net/en_US/fbevents.js

    # in a script: //js.hs-analytics.net/analytics/ -- id is '/\d{6}\\.js'

    return facets


def facets_from_response_headers(headers_list):
    '''
    Extract facets from headers. All are useful for site software fingerprinting but
    for now we'll default to grabbing the most search-enginey ones
    '''
    facets = []
    for h in headers_list:
        k, v = h
        #if k in save_response_headers:
        #    facets.append(('header-'+k, v))
        facets.append(('header-'+k, v))  # XXX save them all for one run

    return facets


# XXX should be generalized using lists from adblockers
def facets_from_embeds(embeds):
    facets = []
    for url in embeds:  # this is both href and src embeds, but whatever
        u = url.url
        if 'cdn.ampproject.org' in u:
            facets.append(('google amp', True))
        if 'www.google-analytics.com' in u:
            # rare that it's used this way
            # XXX parse the publisher id out of the cgi
            facets.append(('google analytics link', True))
        if 'googlesyndication.com' in u:
            facets.append(('google adsense', True))
        if 'google.com/adsense/domains' in u:
            facets.append(('google adsense for domains', True))
        if 'googletagmanager.com' in u:
            facets.append(('google tag manager', True))
            cgi = url.urlsplit.query
            cgi_list = cgi.split('&')
            for c in cgi_list:
                if c.startswith('id=GTM-'):
                    facets.append(('google tag manager id', c[3:]))
        '''
        <script src="//cdn.optimizely.com/js/860020523.js"></script>
        <link rel="shortcut icon" href="//d5y6wgst0yi78.cloudfront.net/images/favicon.ico" />
        <link rel="stylesheet" href="//s3-us-west-1.amazonaws.com/nwusa-cloudfront/font-awesome/css/font-awesome.min.css" />
        <link href='//fonts.googleapis.com/css?family=Open+Sans:400,300' rel='stylesheet' type='text/css'>
        major cdns: Akami, Amazon CloudFront, MaxCDN, EdgeCast, Amazon S3, CloudFlare, Fastly, Highwinds, KeyCDN, Limelight Networks
        '''

    return facets
