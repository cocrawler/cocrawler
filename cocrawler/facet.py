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


def compute_all(html, head, headers, embeds, url=None):
    facets = []
    facets.extend(find_head_facets(head, url=url))
    facets.extend(facets_grep(head))
    facets.extend(facets_from_response_headers(headers))
    facets.extend(facets_from_embeds(embeds))
    facets.extend(facets_from_cookies(headers))

    return facet_dedup(facets)

def find_head_facets(head, url=None):
    '''
    We use html parsing, because the head is smallish and friends don't let
    friends parse html with regexes.

    beautiful soup + lxml2 parses only about 4-16 MB/s
    '''
    facets = []

    # XXX this belongs moved up a couple into the parser
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
            # can also have target= but we don't care
            # TODO base affects all relative URLs in doc
            # XXX when I hoist the soup, hoist this code too
            # can get clues here that www. or https is really the canonical site

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

save_response_headers = ('refresh', 'server', 'set-cookie', 'strict-transport-security', 'x-powered-by')


def facets_from_response_headers(headers):
    '''
    Refresh: N; url=http://...
    Server: ...
    Set-Cookie: ... (note Secure or HttpOnly) (note Secure sent over HTTP)
    Strict-Transport-Security:
    X-Powered-By:
    '''
    facets = []
    for h in headers:
        k, v = h
        if k in save_response_headers:
            facets.append((k, v))

    return facets


# XXX should be generalized using lists from adblockers
def facets_from_embeds(embeds):
    facets = []
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


cookie_matches = {
    'CAKEPHP': 'CAKE PHP framework',
    'ci_session': 'CodeIgniter PHP framework',
    '__cfduid': 'CloudFlare',
    'PHPSESSID': 'PHP',
    'ASP.NET': 'aspx',
    'JSESSIONID': 'java',
    'ldblog_u': 'ldblog_u',
    'bloguid': 'bloguid',
    'XSRF-TOKEN': 'Angular',  # might be true
    'laravel_session': 'laravel',
    'safedog-flow-item': 'safedog',
    'mirtesen': 'mirtesen',
    'csrftoken': 'Django',
    'yunsuo_session_verify': 'yunsuo',
    'AWSELB': 'AWSELB',
    'gvc': 'gvc',
    'CFID': 'ColdFusion',
    'bb_lastvisit': 'vBulletin',
    'ARRAffinity': 'Windows Azure lb',
    'SERVERID': 'HAProxy lb',
    'CMSPreferredCulture': 'Kentico CMS',
    '_icl_current_language': 'WPML multilingual',
    '__RequestVerificationToken': 'aspx',
    'fe_typo_user': 'Typo3 CMS',
    'symfony': 'Symfony PHP framework',
    'EktGUID': 'Ektron CMS',
    'bbsessionhash': 'vBulletin',
    'wordpress_test_cookie': 'WordPress',
    'plack_session': 'perl plack framework',
    'rack.session': 'ruby rack webserver',
    'wpSGCacheBypass': 'SG CachePress Wordpress plugin',
    'BlueStripe.PVN': 'Bluestripe perf monitoring',
}

cookie_prefixes = {
    '.ASPX': 'aspx',
    'AspNet': 'aspx',
    'ASPSESSIONID': 'aspx',
    'BIGipServer': 'F5 BIG-IP',
    'phpbb_': 'PHPBB',
    'phpbb2': 'PHPBB2',
    'phpbb3_': 'PHPBB3',
    'visid_incap_': 'Incapsula Security CDN',  # has a site id
    'wfvt_': 'WordPress Wordfence plugin',
    'X-Mapping-': 'Riverbed Stingray Traffic Manager',
}


def facets_from_cookies(headers):
    facets = []
    for k, v in headers:
        if k != 'set-cookie':
            continue
        key = v.partition('=')[0]
        if key in cookie_matches:
            facets.append((cookie_matches[key], True))
            continue
        for cp in cookie_prefixes:
            if key.startswith(cp):
                facets.append((cookie_prefixes[cp], True))
                break
        else:
            if (len(key) == 32 and
                re.fullmatch(r'[0-9a-f]{32}', key)):
                facets.append(('Mystery 1', True))
            elif (len(key) == 36 and key.startswith('SESS') and
                  re.fullmatch(r'SESS[0-9a-f]{32}', key)):
                facets.append(('Mystery 2', True))
            elif (len(key) == 15 and key.startswith('SN') and
                  re.fullmatch(r'SN[0-9a-f]{13}', key)):
                facets.append(('Mystery 3', True))
            elif (len(key) == 10 and key.startswith('TS') and
                  re.fullmatch(r'TS[0-9a-f]{8}', key)):
                facets.append(('Mystery 4', True))
            elif (len(key) == 42 and key.startswith('wordpress_') and
                  re.fullmatch(r'wordpress_[0-9a-f]{32}', key)):
                facets.append(('WordPress', True))
    return facets
