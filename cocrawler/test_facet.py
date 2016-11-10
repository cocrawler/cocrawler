import pytest

import facet
from urls import URL


def test_double_entries():
    t = '''
    <meta name="robots" content="noarchive" />
    <meta name="robots" content="index, follow" />
    <meta http-equiv="content-type" content="text/html; charset=utf-8">
    <meta charset="utf-8">
    <meta name="referrer" content="unsafe-url">
    <meta name="referrer" content="always">
    <meta name="format-detection" content="telephone=no"/>
    <meta name="format-detection" content="email=no"/>
    '''
    facets = facet.find_head_facets(t)
    assert facets == [('robots', 'noarchive'),
                      ('robots', 'index, follow'),
                      ('referrer', 'unsafe-url'),
                      ('referrer', 'always'),
                      ('format-detection', 'telephone=no'),
                      ('format-detection', 'email=no')]


def test_generator():
    t = '''
    <meta name="generator" content="WordPress 2.5.1" />
    <meta name="generator" content="Movable Type 3.33" />
    <meta name="generator" content="Movable Type Publishing Platform 4.01" />
    <meta name="generator" content="Drupal 7 (http://drupal.org)" />
    '''
    facets = facet.find_head_facets(t)
    assert facets == [('generator', 'WordPress 2.5.1'),
                      ('wordpress', True),
                      ('generator', 'Movable Type 3.33'),
                      ('movable type', True),
                      ('generator', 'Movable Type Publishing Platform 4.01'),
                      ('movable type', True),
                      ('generator', 'Drupal 7 (http://drupal.org)'),
                      ('drupal', True)]


def test_link_rel():
    t = '''
    <link rel="amphtml" href="http://abcnews.go.com/amp/Politics/russia-trump-political-conflict-zone/story?id=42263092" />
    <link rel="alternate" type="application/rss+xml" title="App Links &raquo; FAQs Comments Feed" href="http://applinks.org/faqs/feed/" />
    <link rel="canonical" href="https://www.bloomberg.com/news/articles/2016-10-31/postmates-secures-141-million-in-a-super-super-difficult-fundraising-effort">
    '''
    facets = facet.find_head_facets(t)
    assert facets == [('amphtml',
                       ('http://abcnews.go.com/amp/Politics/russia-trump-political-conflict-zone/story?id=42263092',
                        'notype')),
                      ('alternate', ('http://applinks.org/faqs/feed/', 'application/rss+xml')),
                      ('canonical',
                       ('https://www.bloomberg.com/news/articles/2016-10-31/postmates-secures-141-million-in-a-super-super-difficult-fundraising-effort',
                        'notype'))]


def test_facebook():
    t = '''
    <meta property="fb:admins" content="704409894" />
    <meta property="fb:app_id" content="4942312939" />
    <meta property="og:site_name" content="ABC News" />
    <link rel="opengraph" href="..." />
    <link rel="origin" href="..."/>
    <meta property="op:markup_version" content="v1.0">
    '''
    facets = facet.find_head_facets(t)
    assert facets == [('fb:admins', '704409894'),
                      ('fb:app_id', '4942312939'),
                      ('opengraph', True),
                      ('fb instant', True),
                      ('opengraph', ('...', 'notype')),
                      ('origin', ('...', 'notype'))]


def test_twitter():
    t = '''
    <meta property="twitter:card" content="summary_large_image" />
    <meta property="twitter:site" content="@ABC" />
    <meta property="twitter:creator" content="@brianross" />
    <meta name="twitter:app:id:iphone" content="300255638" />
    <meta name="twitter:app:url:iphone" content="abcnewsiphone://link/story,42263092" />
    <meta name="twitter:app:name:ipad" content="ABC News" />
    <meta name="twitter:app:id:ipad" content="306934135" />
    '''
    facets = facet.find_head_facets(t)
    assert facets == [('twitter card', True), ('twitter card', True),
                      ('twitter card', True), ('twitter card', True),
                      ('twitter:site', '@ABC'),
                      ('twitter:creator', '@brianross')]

    facets = facet.facet_dedup(facets)
    assert facets == [('twitter card', True),
                      ('twitter:site', '@ABC'),
                      ('twitter:creator', '@brianross')]


def test_applinks():  # fb + Parse
    t = '''
    <meta property="al:ios:url" content="applinks://docs" />
    <meta property="al:ios:app_store_id" content="12345" />
    <meta property="al:ios:app_name" content="App Links" />
    '''
    facets = facet.find_head_facets(t)
    assert facets == [('applinks', True), ('applinks', True), ('applinks', True)]


def test_misc_meta_name():
    t = '''
    <meta name="parsely-title" content="Postmates Secures $141 Million in a ‘Super, Super Difficult’ Fundraising Effort">
    '''
    facets = facet.find_head_facets(t)
    assert facets == [('parsely-title',
                       'Postmates Secures $141 Million in a ‘Super, Super Difficult’ Fundraising '
                       'Effort')]


@pytest.mark.skip(reason='not yet implemented')
def test_google_stuff():
    t = '''
    <script type="text/javascript" defer="defer" async="async" src="//www.google-analytics.com/analytics.js?oeorvp"></script>
    <script src="http://www.google.com/adsense/domains/caf.js"></script>
    <script type="text/javascript" src="http://pagead2.googlesyndication.com/pagead/show_ads.js">
    '''
    facets = facet.find_head_facets(t)
    assert facets == 'foo'


def test_integrity():
    t = '''
    <script src="https://example.com/example-framework.js"
        integrity="sha384-Li9vy3DqF8tnTXuiaAJuML3ky+er10rcgNR/VqsVpcw+ThHmYcwiB1pbOxEbzJr7"
        crossorigin="anonymous"></script>

    <script src="hello_world.js"
    integrity="sha384-dOTZf16X8p34q2/kYyEFm0jh89uTjikhnzjeLeF0FHsEaYKb1A1cv+Lyv4Hk8vHd
              sha512-Q2bFTOhEALkN8hOms2FKTDLy7eugP2zFZ1T8LCvX42Fp3WoNr3bjZSAHeOsHrbV1Fu9/A0EzCinRE7Af1ofPrw=="
    crossorigin="anonymous"></script>

    <link rel="opengraph" href="http://example.com"
        integrity="sha384-Li9vy3DqF8tnTXuiaAJuML3ky+er10rcgNR/VqsVpcw+ThHmYcwiB1pbOxEbzJr7" />

    <link rel="amphtml" href="http://example.com/amp" />
    '''
    facets = facet.find_head_facets(t)
    assert facets == [('opengraph', ('http://example.com', 'notype')),
                      ('amphtml', ('http://example.com/amp', 'notype')),
                      ('script integrity', 3)]


def test_facets_grep():
    t = '''
    # 3 different styles of configuring google analytics
    ga('create', 'UA-63787687-1', 'auto');
    var pageTracker = _gat._getTracker("UA-8162380-2"); # Old
    _gaq.push(['_setAccount', 'UA-1234567-6']);

    # adense embeds the external script name in inline js
    google_ad_client = "pub-5692821333050410"; # publisher ID
    '''
    facets = facet.facets_grep(t)
    assert facets == [('google publisher id', 'pub-5692821333050410'),
                      ('google analytics', 'UA-63787687-1'),
                      ('google analytics', 'UA-8162380-2'),
                      ('google analytics', 'UA-1234567-6')]


def test_misc():
    t = '''
    <html lang="fr">
    '''
    facets = facet.find_head_facets(t)
    assert facets == [('html lang', 'fr')]
    t = '''
    <html xml:lang="fr" xmlns="http://www.w3.org/1999/xhtml">
    '''
    facets = facet.find_head_facets(t)
    assert facets == [('html xml:lang', 'fr')]
    t = '''
    <base href="http://example.com/">
    '''
    facets = facet.find_head_facets(t)
    assert facets == [('base', 'http://example.com/')]


def test_response_header_facets():
    h = (('server', 'Foo'),)
    facets = facet.facets_from_response_headers(h)
    assert facets == [('server', 'Foo')]


def test_facets_from_embeds():
    embeds = set((URL('http://example.com'), URL('http://cdn.ampproject.org')))
    facets = facet.facets_from_embeds(embeds)
    assert facets == [('google amp', True)]

def test_facets_from_cookies_mysteries():
    mysteries = {'0b2a2ea1719965db9784044304710a9c': 'Mystery 1',
                 'SESS0084bfbcced439ebac5f559df8dd21e0': 'Mystery 2',
                 'SN54bceb45e3fd0': 'Mystery 3',
                 'TS0103d65d': 'Mystery 4',
                 'wordpress_07dcfa44f6c1a509a602e92e749c7b6d': 'WordPress'}
    for m in mysteries:
        f = facet.facets_from_cookies((('set-cookie', m+'=foo'),))
        assert f[0][0] == mysteries[m]


def test_facets_from_cookies_matches():
    f = facet.facets_from_cookies((('set-cookie', 'PHPSESSID=foo'),))
    assert f[0][0] == 'PHP'


def test_facets_from_cookies_prefixes():
    f = facet.facets_from_cookies((('set-cookie', 'phpbb_2354'),))
    assert f[0][0] == 'PHPBB'


# ----------------------------------------------------------------------
# A collection of stuff I may get to later
# ----------------------------------------------------------------------

# perhaps collect google tag manager IDs? these are multi-page
'''
    <iframe src="https://www.googletagmanager.com/ns.html?id=GTM-M9L9Q5&gtm_auth=GTM-M9L9Q5" height="0" width="0" style="display:none;visibility:hidden">
    # I think gtm_auth can be ignored?

    # js version
    })(window,document,'script','dataLayer','GTM-XXXX');</script>

'''

# fb like button? script with
'''
var appId = "1610172482603249";
'''

# a kind of redir
'''
<meta http-equiv="refresh" content="0; url=http://www.ExampleOnly.com/"/>
'''

# some M$-specific pin-to-taskbar thing
'''
<meta name="msapplication-task" content="name=Search;action-uri=http://query.nytimes.com/search/sitesearch?src=iepin;icon-uri=https://static01.nyt.com/images/icons/search.ico" />
'''

# appears to be NYT specific?
'''
<meta name="sourceApp" content="nyt-v5" />
<meta name="video:playerId" content="2640832222001" />
<meta name="video:publisherId" content="1749339200" />
<meta name="video:publisherReadToken" content="cE97ArV7TzqBzkmeRVVhJ8O6GWME2iG_bRvjBTlNb4o." />
<meta name="adxPage" content="homepage.nytimes.com/index.html" />
'''
