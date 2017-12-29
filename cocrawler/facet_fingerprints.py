'''
Bulk data for fingerprinting webserver facets
'''

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
    'visid_incap_': 'Incapsula Security CDN',  # XXX has a site id worth extracting
    'wfvt_': 'WordPress Wordfence plugin',
    'X-Mapping-': 'Riverbed Stingray Traffic Manager',
}
