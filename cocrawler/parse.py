'''
Parse links in html and css pages.
'''

import logging
import re
import hashlib

from bs4 import BeautifulSoup

from . import stats
from .urls import URL
from . import facet

LOGGER = logging.getLogger(__name__)


def do_burner_work_html(html, html_bytes, headers_list, url=None):
    stats.stats_sum('parser html bytes', len(html_bytes))

    # This embodies a minimal parsing policy; it needs to be made pluggable/configurable
    #  split head/body
    #  soup the head so we can accurately get base and facets
    #  regex the body for links and embeds, for speed

    with stats.record_burn('split_head_body', url=url):
        head, body = split_head_body(html)

    with stats.record_burn('head soup', url=url):
        try:
            head_soup = BeautifulSoup(head, 'lxml')
        except Exception as e:
            LOGGER.info('url %s threw the %r exception in BeautifulSoup', url, e)
            # TODO: if common, we need to recover not skip
            raise

    base = head_soup.find('base') or {}
    base = base.get('href')

    with stats.record_burn('find_head_links_soup', url=url):
        links, embeds = find_head_links_soup(head_soup)

    with stats.record_burn('find_body_links_re', url=url):
        lbody, ebody = find_body_links_re(body)
        links.update(lbody)
        embeds.update(ebody)

    with stats.record_burn('url_clean_join', url=url):
        links = url_clean_join(links, url=base or url)
        embeds = url_clean_join(embeds, url=base or url)

    with stats.record_burn('sha1 html', url=url):
        sha1 = 'sha1:' + hashlib.sha1(html_bytes).hexdigest()

    with stats.record_burn('facets', url=url):
        # find_html_links doesn't actually produce embeds,
        # so we're going to parse links for now XXX config
        facets = facet.compute_all(html, head, headers_list, links)

    return links, embeds, sha1, facets


def find_html_links_re(html):
    '''
    Find the outgoing links and embeds in html, body head and body.
    This can't tell the difference between links and embeds, so we
    call them all links.

    On a 3.4ghz x86 core, this runs at 50 megabytes/sec. XXX update for 2 regex
    '''
    stats.stats_sum('html parser bytes', len(html))

    delims = set(
        [m[1] for m in re.findall(r'''\s(?:href|src)\s{,3}=\s{,3}(?P<delim>['"])(.*?)(?P=delim)''', html, re.I | re.S)]
    )
    no_delims = set(re.findall(r'''\s(?:href|src)\s{,3}=\s{,3}([^\s'"<>]+)''', html, re.I))

    links = delims.union(no_delims)

    return links, set()


def find_body_links_re(body):
    '''
    Find links in html, divided among links and embeds.

    On a 3.4 ghz x86 core, runs around 25 megabyte/sec XXX update now that there are 4 regex
    '''
    stats.stats_sum('html parser body bytes', len(body))

    embeds_delims = set(
        [m[1] for m in re.findall(r'''\ssrc\s{,3}=\s{,3}(?P<delim>['"])(.*?)(?P=delim)''', body, re.I | re.S)]
    )
    embeds_no_delims = set(re.findall(r'''\ssrc\s{,3}=\s{,3}([^\s'"<>]+)''', body, re.I))
    embeds = embeds_delims.union(embeds_no_delims)
    links_delims = set(
        [m[1] for m in re.findall(r'''\shref\s{,3}=\s{,3}(?P<delim>['"])(.*?)(?P=delim)''', body, re.I | re.S)]
    )
    links_no_delims = set(re.findall(r'''\shref\s{,3}=\s{,3}([^\s'"<>]+)''', body, re.I))
    links = links_delims.union(links_no_delims)

    return links, embeds


def find_css_links_re(css):
    '''
    Finds the links embedded in css files
    '''
    stats.stats_sum('html parser css bytes', len(css))

    embeds_delims = set(
        [m[1] for m in re.findall(r'''\surl\(\s?(?P<delim>['"])(.*?)(?P=delim)''', css, re.I | re.S)]
    )
    embeds_no_delims = set(re.findall(r'''\surl\(\s?([^\s'"<>()]+)''', css, re.I))

    return set(), embeds_delims.union(embeds_no_delims)


def find_head_links_soup(head_soup):
    embeds = set()
    for tag in head_soup.find_all(src=True):
        embeds.add(tag.get('src'))
    for tag in head_soup.find_all(href=True):
        embeds.add(tag.get('href'))
    return set(), embeds


def find_body_links_soup(body_soup):
    embeds = set()
    links = set()
    for tag in body_soup.find_all(src=True):
        embeds.add(tag.get('src'))
    for tag in body_soup.find_all(href=True):
        links.add(tag.get('href'))
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


def split_head_body(html):
    '''
    Efficiently split the head from the body, so we can use different
    parsers on each.  There's no point doing this split if it's
    expensive.

    It's legal for webpages to leave off <head> and <body>; the
    standard requires browsers to figure it out based on the html
    tags. We can't do that efficiently, so we punt for such webpages,
    and return the entire page as body.
    '''

    # hueristic: if there's a <head> tag at all, it's early in the document
    top = html[:1000]
    m = re.search(r'<head>', top, re.I)
    if not m:
        return '', html

    # hueristic: the head is not overly-large
    small = html
    if len(html) > 100000:
        small = small[:100000]

    m = re.search(r'<(?:/head>|body[\s>])', small, re.I)
    if not m:
        return '', html

    print(m.start, m.end)
    return html[:m.start()], html[m.end():]


'''
Helpers to minimize how many bytes we have to html parse.
Of course, these are all dangerous, but they might be useful
if the <head> of a webpage is abnormally large
'''


def regex_out_comments(html):
    return re.sub('<!--.*?-->', '', html, flags=re.S)


def regex_out_some_scripts(html):
    '''
    This nukes <script>...</script>, but does not nuke <script type="...
    '''
    return re.sub('<script>.*?</script>', '', html, flags=re.S)


def regex_out_all_scripts(html):
    return re.sub('<script[ >].*?</script>', '', html, flags=re.S)
