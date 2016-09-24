'''
Parse links in html and css pages.

XXX also need a gumbocy version
'''

import logging
import asyncio
import re
import functools
import time
import urllib.parse

import stats
import urls

LOGGER = logging.getLogger(__name__)

async def find_html_links_async(html, executor, loop, url=None):
    stats.stats_sum('parser html bytes', len(html))

    fhl = functools.partial(find_html_links, html, url=url)
    wrap = functools.partial(stats_cpu_wrap, fhl, 'parser cpu time')

    f = asyncio.ensure_future(loop.run_in_executor(executor, wrap))
    with stats.coroutine_state('find_html_links asyncer'):
        l, e, s = await f

    for key in s.get('stats', {}):
        stats.stats_sum(key, s['stats'][key])

    return l, e

def stats_cpu_wrap(partial, name):
    c0 = time.clock()
    ret = list(partial())
    ret.append({'stats': {name: time.clock() - c0}})
    return ret

def report():
    b = stats.stat_value('parser html bytes')
    c = stats.stat_value('parser cpu time')
    LOGGER.info('Parser thread report:')
    if c is not None and c > 0:
        LOGGER.info('  Parser parsed %.1f MB/cpu-second', b / c / 1000000)

# ----------------------------------------------------------------------

def find_html_links(html, url=None):
    '''
    Find the outgoing links and embeds in html. If url passed in, urljoin to it.

    On a 3.4ghz x86 core, this runs at 50 megabytes/sec (20ms per MB)
    '''
    with stats.record_burn('find_html_links re', url=url): # this with is currently a noop
        links = set(re.findall(r'''\s(?:href|src)=['"]?([^\s'"<>]+)''', html, re.I))

    with stats.record_burn('find_html_links cleanjoin', url=url): # this with is currently a noop
        ret = set()
        for u in links:
            u = urls.clean_webpage_links(u)
            if url is not None:
                u = urllib.parse.urljoin(url, u)
            ret.add(u)

    return ret, set()

def find_html_links_and_embeds(html, url=None):
    '''
    Find links in html, divided among links and embeds.
    More expensive than just getting unclassified links - 38 milliseconds/megabyte @ 3.4 ghz x86
    '''
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

    return links_body, embeds

def find_css_links(css, url=None):
    '''
    Finds the links embedded in css files
    '''

    with stats.record_burn('find_css_links re', url=url):
        ret = set(re.findall(r'''\surl\(\s?['"]?([^\s'"<>()]+)''', css, re.I))

    return ret, set()
