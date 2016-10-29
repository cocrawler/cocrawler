'''
Parse links in html and css pages.

XXX also need a gumbocy version
'''

import logging
import re
import hashlib

import stats
from urls import URL

LOGGER = logging.getLogger(__name__)

def do_burner_work_html(html, html_bytes, url=None):
    stats.stats_sum('parser html bytes', len(html_bytes))

    with stats.record_burn('find_html_links re', url=url):
        links, embeds = find_html_links(html, url=url)

    with stats.record_burn('find_html_links url_clean_join', url=url):
        links = url_clean_join(links, url=url)
        embeds = url_clean_join(embeds, url=url)

    with stats.record_burn('sha1 html', url=url):
        sha1 = 'sha1:' + hashlib.sha1(html_bytes).hexdigest()

    return links, embeds, sha1

def find_html_links(html, url=None):
    '''
    Find the outgoing links and embeds in html. If url passed in, urljoin to it.

    On a 3.4ghz x86 core, this runs at 50 megabytes/sec.
    '''
    links = set(re.findall(r'''\s(?:href|src)=['"]?([^\s'"<>]+)''', html, re.I))
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
        links = set(re.findall(r'''\surl\(\s?['"]?([^\s'"<>()]+)''', css, re.I))

    links = url_clean_join(links, url=url)
    return links, set()

def url_clean_join(links, url=None):
    ret = set()
    for u in links:
        ret.add(URL(u, urljoin=url))
    return ret

def report():
    b = stats.stat_value('parser html bytes')
    c = stats.stat_value('find_html_links re')
    LOGGER.info('Burner thread report:')
    if c is not None and c > 0:
        LOGGER.info('  Burner thread parsed %.1f MB/cpu-second', b / c / 1000000)
    d = stats.stat_value('sha1 html')
    if d is not None and d > 0:
        LOGGER.info('  Burner thread sha1 %.1f MB/cpu-second', b / d / 1000000)

    t, c = stats.burn_values('find_html_links url_clean_join')
    if c is not None and c > 0 and t is not None and t > 0:
        LOGGER.info('  Burner thread cleaned %.1f kilo-urls/cpu-second', c / t / 1000)
