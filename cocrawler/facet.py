'''
Code related to generating webpage facets.

For normal crawling, we only parse facets we think might be useful
for crawling and ranking: STS, twitter cards, facebook opengraph.

TODO: find rss feeds (both link alternate and plain href to .xml)
TODO: probe with DNT:1 and see who replies TK: N

This module also contains code to post-facto process headers to
figure out what technologies are used in a website.

'''

import re

from bs4 import BeautifulSoup

from . import stats

meta_name_content = set(('twitter:site', 'twitter:site:id', 'twitter:creator', 'twitter:creator:id',
                         'robots', 'charset', 'http-equiv', 'referrer', 'format-detection', 'generator',
                         'parsely-title'))
meta_name_generator_special = ('wordpress', 'movable type', 'drupal')
meta_name_prefix = (('twitter:', 'twitter card'),)

meta_property_content = set(('twitter:site', 'twitter:site:id', 'twitter:creator', 'twitter:creator:id',
                             'fb:app_id', 'fb:admins'))
meta_property_prefix = (('al:', 'applinks'),
                        ('og:', 'opengraph'),
                        ('article:', 'opengraph'),
                        ('op:', 'fb instant'),
                        ('bt:', 'boomtrain'),)

meta_link_rel = set(('canonical', 'alternate', 'amphtml', 'opengraph', 'origin'))

save_response_headers = ('refresh', 'server', 'set-cookie', 'strict-transport-security',)


def compute_all(html, head, headers_list, embeds, url=None):
    facets = []
    facets.extend(find_head_facets(head, url=url))
    facets.extend(facets_grep(head))
    facets.extend(facets_from_response_headers(headers_list))
    facets.extend(facets_from_embeds(embeds))
    facets.extend(facets_from_cookies(headers_list))

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
        try:
            soup = BeautifulSoup(head, 'html.parser')
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
            # TODO base affects all relative URLs in doc
            # XXX when I hoist the soup, hoist this code too
            # can get clues here that www. or https is really the canonical site

    meta = soup.find_all('meta', attrs={'name': True})  # 'name' collides, so use dict
    for m in meta:
        n = m.get('name').lower()
        if n in meta_name_content:
            facets.append((n, m.get('content')))
        if n == 'generator':
            g = m.get('content', '')
            gl = g.lower()
            for s in meta_name_generator_special:
                if s in gl:
                    facets.append((s, True))
        for pre in meta_name_prefix:
            prefix, title = pre
            if n.startswith(prefix):
                facets.append((title, True))

    meta = soup.find_all('meta', property=True)
    for m in meta:
        p = m.get('property').lower()
        if p in meta_property_content:
            facets.append((p, m.get('content')))
        for pre in meta_property_prefix:
            prefix, title = pre
            if p.startswith(prefix):
                facets.append((title, True))

    # link rel is muli-valued attribute, hence, a list
    linkrel = soup.find_all('link', rel=True)
    for l in linkrel:
        for rel in l.get('rel'):
            r = rel.lower()
            if r in meta_link_rel:
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


def facets_from_response_headers(headers_list):
    '''
    Extract facets from headers. All are useful for site software fingerprinting but
    for now we'll default to grabbing the most search-enginey ones
    '''
    facets = []
    for h in headers_list:
        k, v = h
        if k in save_response_headers:
            facets.append(('header-'+k, v))

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
         this appears in the body, so...
         is there a pure js version of gtm?
        <script src="//cdn.optimizely.com/js/860020523.js"></script>
        <link rel="shortcut icon" href="//d5y6wgst0yi78.cloudfront.net/images/favicon.ico" />
        <link rel="stylesheet" href="//s3-us-west-1.amazonaws.com/nwusa-cloudfront/font-awesome/css/font-awesome.min.css" />
        <link href='//fonts.googleapis.com/css?family=Open+Sans:400,300' rel='stylesheet' type='text/css'>
        major cdns: Akami, Amazon CloudFront, MaxCDN, EdgeCast, Amazon S3, CloudFlare, Fastly, Highwinds, KeyCDN, Limelight Networks
        '''

    return facets


cookie_matches = {
    'CAKEPHP': 'CAKE PHP framework',
    'ci_session': 'CodeIgniter PHP framework',
    '__cfduid': 'cloudflare',
    '__jsluid': 'jiasule',
    'PHPSESSID': 'PHP',
    'ASP.NET': 'aspx',
    '__RequestVerificationToken': 'aspx',
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
    'bbsessionhash': 'vBulletin',
    'ARRAffinity': 'Windows Azure loadbalancer',
    'SERVERID': 'HAProxy loadbalancer',
    'CMSPreferredCulture': 'Kentico CMS',
    '_icl_current_language': 'WPML multilingual',
    'fe_typo_user': 'Typo3 CMS',
    'symfony': 'Symfony PHP framework',
    'EktGUID': 'Ektron CMS',
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
    'BIGipServer': 'BIG-IP Application Security Manager (F5)',
    'phpbb_': 'PHPBB',
    'phpbb2': 'PHPBB2',
    'phpbb3_': 'PHPBB3',
    'visid_incap_': 'Incapsula Security CDN',  # has a site id
    'wfvt_': 'WordPress Wordfence plugin',
    'X-Mapping-': 'Riverbed Stingray Traffic Manager',
}


def facets_from_cookies(headers_list):
    facets = []
    for k, v in headers_list:
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
            if ((len(key) == 32 and
                 re.fullmatch(r'[0-9a-f]{32}', key))):
                facets.append(('Mystery 1', True))
            elif (len(key) == 36 and key.startswith('SESS') and
                  re.fullmatch(r'SESS[0-9a-f]{32}', key)):
                facets.append(('Mystery 2', True))
            elif (len(key) == 15 and key.startswith('SN') and
                  re.fullmatch(r'SN[0-9a-f]{13}', key)):
                facets.append(('Mystery 3', True))
            elif (len(key) == 10 and key.startswith('TS') and
                  re.fullmatch(r'TS[0-9a-f]{8}', key)):
                facets.append(('BIG-IP Application Security Manager (F5)', True))
            elif (len(key) == 42 and key.startswith('wordpress_') and
                  re.fullmatch(r'wordpress_[0-9a-f]{32}', key)):
                facets.append(('WordPress', True))
    return facets

'''
go through headers and save more headers (grep 'not saving')
 link
 p3p
 content-security-policy
 timing-allow-origin
 x-ua-compatible  # IE method of selecting which mode for rendering
 x-xss-protection
 x-pingback  # blog of some kind, not necessarily wordpress
 x-runtime  # generic header for timing info
 x-robots-tag
 x-served-by  # generic load-balancing header
 x-server  # generally a load-balancer, not that interesting ...
 x-host  # load balancing? not interesting
 servedby  # some kind of load-balancing header for a particular hosting company
 Tk  # DNT response, "Tk: N" means not tracking.
 x-frame-option  # values like sameorigin, deny, allow-from uri, blah blah

 x-aspnet-version  # asp.net
 x-aspnetmvc-version  # asp.net
 ms-author-via  # value might have "DAV" for WebDAV and/or "MS-FP" for Microsoft FrontPage protocol
 x-drupal-*  # drupal
 x-via  # value frequently mentions "Cdn Cache Server"
 x-mod-pagespeed  # likely Apache with mod_pagespeed
 x-page-speed  # likely nginx with mod_pagespeed
 x-cdn  # most commonly value="Incapsula"
 x-iinfo  # incapsula
 eagleid  # related to tengine?
 x-swift-savetime  # nginx swift proxy C++ module https://github.com/OSLL/nginx-swift
 x-swift-cachetime  # nginx swift proxy C++ module https://github.com/OSLL/nginx-swift
 x-turbo-charged-by  # value=LigitSpeed
 x-ua-device  # sent by Varnish to backend, appears to leak out?
 x-ah-environment  # Varnish and Acquia.com ?
 xkey  # Varnish xkey module
 via  # values like "1.1 varnish" which means http 1.1 and varnish is the software... little used except by varnish
 x-varnish-*  # Varnish cache
 x-hits  # Varnish
 x-powered-by-plesk  # Plesk WebOps platform
 x-timer  # fastly cached asset ?
 fastly-debug-digest  # fastly
 surrogate-key  # fastly
 surrogate-keys  # fastly
 x-akami-transformed  # Akami CDN
 x-rack-cache  # cache for Ruby Rack
 wp-super-cache  # WP Super Cache plugin for Wordpress
 x-powered-cms  # value=Bitrix Site Manager
 cf-railgun  # CloudFlare RailGun site
 x-info-cf-ray  # CloudFlare
 cf-cache-status  # CloudFlare
 cf-ray  # CloudFlare
 cf-cache-status  # CloudFlare
 x-amz-cf-id  # Amazon CloudFront
 x-amz-id-1  # Amazon AWS debugging info
 x-amz-id-2  # Amazon AWS debugging info
 x-amz-request-id  # Amazon S3
 x-amz-version-id  # Amazon S3 versioned object
 x-amz-delete-marker  # Amazon S3 tombstone (true/false)
 x-content-encoded-by  # {Joomla,Dimofinf Cms 3.0.0}
 x-content-powered-by  # value="K2 ... (by JoomlaWorks)"
 x-sucuri-id  # Sucuri website security
 x-px  # CDNetworks
 px-uncompress-origin
 x-litespeed-cache # LiteSpeed Cache WordPress plugin
 x-safe-firewall  # value contains 'webscan.360.cn' == 360webscan
 x-powered-by-360wzb  # 360wzb
 x-powered-by-anquanbao  # anquanbao
 powered-by-chinacache  # ChinaCache CDN
 x-instart-request-id  # Instart Logic Application Delivery Solution
 x-styx-version  # Pantheon Styx edge router, value=StyxGo
 x-styx-req-id  # Pantheon Styx edge router
 x-pantheon-styx-hostname
 x-pantheon-endpoint
 x-cloud-trace-context  # Google App Engine Stackdriver Trace
 x-pad  # old Apache server
 x-dynatrace-js-agent  # Dynatrace Application Performance Management
 x-dynatrace  # Dynatrace Application Performance Management
 dynatrace
 liferay-portal  # "Community Edition", "Enterprise Edition" Enterprise web platform
 x-framework # value="JP" or "Samurai" (PHP Full stack framework)
 cc-cache  # CC-Cache Wordpress plugin
 x-xrds-location  # Yadis service discovery
 x-atg-version  # Oracle ATG Web Commerce
 x-spip-cache  # SPIP CMS
 composed-by  # value contains SPIP, SPIP CMS
 x-hyper-cache  # Hyper Cache WordPress plugin
 x-clacks-overhead  # tribute to Terry Prachett
 x-do-esi  # Edge Side Includes, standard implemented by CDNs and caching proxies
 rtss  # IBM Tivoli runtime security services
 sprequestguid  # Microsoft SharePoint
 microsoftsharepointteamservices  # Microsoft SharePoint
 sprequestduration  # Microsoft SharePoint
 spiislatency  # Microsoft SharePoint
 x-sharepointhealthscore  # Microsoft SharePoint
 x-yottaa-*  # Yottaa eCommerce Acceleration
 commerce-server-software  # value=Microsoft Commerce Server
 tp-cache  # Travelport Cache ??
 tp-l2-cache  # ??

# https://github.com/sqlmapproject/sqlmap/tree/master/waf -- web application firewall fingerprints

 referrer-policy  # rare so far
 retry-after  # goes with a 503, is HTTP-date | delta-seconds ... also 3XX
 tcn choice  # transparent content negotiation, eh?

 x-dw-request-base-id  # ???

 server: ECS == Edgecast CDN
 server: Windows-Azure-Blob == Azure CDN
 server: PWS == CDNetworks


'''
