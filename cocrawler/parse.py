'''
Parse links in html and css pages.

XXX also need a gumbocy version
'''

import logging
import re
import hashlib

from bs4 import BeautifulSoup

import stats
from urls import URL
import facet

LOGGER = logging.getLogger(__name__)


def do_burner_work_html(html, html_bytes, headers, url=None):
    stats.stats_sum('parser html bytes', len(html_bytes))

    with stats.record_burn('find_html_links re', url=url):
        links, embeds = find_html_links(html, url=url)

    with stats.record_burn('find_html_links url_clean_join', url=url):
        links = url_clean_join(links, url=url)
        embeds = url_clean_join(embeds, url=url)

    with stats.record_burn('sha1 html', url=url):
        sha1 = 'sha1:' + hashlib.sha1(html_bytes).hexdigest()

    with stats.record_burn('facets', url=url):
        # TODO get the 'head' from above so I don't have to compute it twice
        head, _ = split_head_body_re(html)
        # find_html_links doesn't actually produce embeds,
        # so we're going to parse links for now XXX config
        facets = facet.compute_all(html, head, headers, links)

    return links, embeds, sha1, facets


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
        head, body = split_head_body_re(html)
        embeds_head = set(re.findall(r'''\s(?:href|src)=['"]?([^\s'"<>]+)''', head, re.I))
        embeds_body = set(re.findall(r'''\ssrc=['"]?([^\s'"<>]+)''', body, re.I))
        links = set(re.findall(r'''\shref=['"]?([^\s'"<>]+)''', body, re.I))
    embeds = embeds_head.union(embeds_body)

    links = url_clean_join(links, url=url)
    embeds = url_clean_join(embeds, url=url)
    return links, embeds


def find_css_links(css, url=None):
    '''
    Finds the links embedded in css files
    '''
    with stats.record_burn('find_css_links re', url=url):
        links = set(re.findall(r'''\surl\(\s?['"]?([^\s'"<>()]+)''', css, re.I))

    links = url_clean_join(links, url=url)
    return links, set()


def soup_and_find(html, url=None):
    head, body = split_head_body_re(html)
    head_soup = BeautifulSoup(head)
    body_soup = BeautifulSoup(body)
    return find_links_from_soup(head_soup, body_soup, url=url)


def find_links_from_soup(head_soup, body_soup, url=None):
    links = set()
    embeds = set()
    for tag in head_soup.find_all(src=True):
        embeds.add(tag.get('src'))
    for tag in head_soup.find_all(href=True):
        embeds.add(tag.get('href'))
    for tag in body_soup.find_all(src=True):
        embeds.add(tag.get('src'))
    for tag in body_soup.find_all(href=True):
        links.add(tag.get('href'))

    links = url_clean_join(links, url=url)
    embeds = url_clean_join(embeds, url=url)
    return links, embeds


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


def split_head_body_re(html):
    try:
        head, body = html.split('<body>', maxsplit=1)
    except ValueError:
        try:
            head, body = html.split('</head>', maxsplit=1)
        except ValueError:
            head = ''
            body = html
    return head, body

# try to minimize how many bytes we have to html parse
# of course, these are all dangerous, but they might be useful
# if the <head> of a webpage is abnormally large


def regex_out_comments(html):
    return re.sub('<!--.*?-->', '', html, flags=re.S)


def regex_out_some_scripts(html):
    '''
    This nukes most inline scripts... although some are <script type="...
    '''
    return re.sub('<script>.*?</script>', '', html, flags=re.S)


def regex_out_all_scripts(html):
    return re.sub('<script[ >].*?</script>', '', html, flags=re.S)
