'''
Parse links in html and css pages.

XXX also need a gumbocy version
'''

import logging
import asyncio
import re
import urllib.parse

import stats
import urls

LOGGER = logging.getLogger(__name__)

def find_html_links(html, url=None):
    '''
    Find the outgoing links and embeds in html. If url passed in, urljoin to it.

    On a 3.4ghz x86 core, this runs at 50 megabytes/sec (20ms per MB)
    '''
    stats.stats_sum('parser html bytes', len(html))

    with stats.record_burn('find_html_links re', url=url): # this with is currently a noop
        links = set(re.findall(r'''\s(?:href|src)=['"]?([^\s'"<>]+)''', html, re.I))

    links = url_clean_join(links, url=url)
    return links, set()

def find_html_links_and_embeds(html, url=None):
    '''
    Find links in html, divided among links and embeds.
    More expensive than just getting unclassified links - 38 milliseconds/megabyte @ 3.4 ghz x86
    '''
    stats.stats_sum('parser html bytes', len(html))

    with stats.record_burn('find_html_links_and_embeds re', url=url):
        try:
            head, body = html.split('<body>', maxsplit=1)
        except ValueError:
            try:
                head, body = html.split('</head>', maxsplit=1)
            except ValueError:
                head = ''
                body = html
        embeds_head = set(re.findall(r'''\s(?:href|src)=['"]?([^\s'"<>]+)''', head, re.I))
        embeds_body = set(re.findall(r'''\ssrc=['"]?([^\s'"<>]+)''', body, re.I))
        links_body = set(re.findall(r'''\shref=['"]?([^\s'"<>]+)''', body, re.I))
    embeds = embeds_head.union(embeds_body)

    links_body = url_clean_join(links_body, url=url)
    embeds = url_clean_join(embeds, url=url)
    return links_body, embeds

def find_css_links(css, url=None):
    '''
    Finds the links embedded in css files
    '''
    with stats.record_burn('find_css_links re', url=url):
        ret = set(re.findall(r'''\surl\(\s?['"]?([^\s'"<>()]+)''', css, re.I))

    return ret, set()

def url_clean_join(links, url=None):
    with stats.record_burn('find_html_links url_clean_join', url=url):
        ret = set()
        for u in links:
            u = urls.clean_webpage_links(u)
            if url is not None:
                u = urllib.parse.urljoin(url, u)
            u, _ = urls.safe_url_canonicalization(u) # XXX I'm discarding the frag here
            ret.add(u)

    return ret

def report():
    b = stats.stat_value('parser html bytes')
    c = stats.stat_value('find_html_links re')
    LOGGER.info('Burner thread report:')
    if c is not None and c > 0:
        LOGGER.info('  Burner thread parsed %.1f MB/cpu-second', b / c / 1000000)

    t, c = stats.burn_values('find_html_links url_clean_join')
    if c is not None and c > 0:
        LOGGER.info('  Burner thread cleaned %.1f kilo-urls/cpu-second', c / t / 1000)
