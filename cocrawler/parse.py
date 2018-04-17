'''
Parse links in html and css pages.
'''

import logging
import re
import hashlib
import urllib.parse

from bs4 import BeautifulSoup

from . import stats
from .urls import URL
from . import facet

LOGGER = logging.getLogger(__name__)


def do_burner_work_html(html, html_bytes, headers, burn_prefix='', url=None):
    stats.stats_sum('parser html bytes', len(html_bytes))

    # This embodies a minimal parsing policy; it needs to be made pluggable/configurable
    #  split head/body
    #  soup the head so we can accurately get base and facets
    #  regex the body for links and embeds, for speed

    with stats.record_burn(burn_prefix+'split_head_body', url=url):
        head, body = split_head_body(html, url=url)

    '''
    beautiful soup + lxml2 parses only about 4-16 MB/s
    '''
    stats.stats_sum('head soup bytes', len(head))
    with stats.record_burn(burn_prefix+'head soup', url=url):
        try:
            head_soup = BeautifulSoup(head, 'lxml')
        except Exception as e:
            LOGGER.info('url %s threw the %r exception in BeautifulSoup', url, e)
            # TODO: if common, we need to recover not skip
            raise

    base = head_soup.find('base') or {}
    base = base.get('href')
    if base:
        # base can be relative, e.g. 'subdir/'
        base = urllib.parse.urljoin(url.url, base)
    base_or_url = base or url

    with stats.record_burn(burn_prefix+'find_head_links_soup', url=url):
        links, embeds = find_head_links_soup(head_soup)

    with stats.record_burn(burn_prefix+'find_body_links_re', url=url):
        lbody, ebody = find_body_links_re(body)
        links.update(lbody)
        embeds.update(ebody)

    with stats.record_burn(burn_prefix+'url_clean_join', url=url):
        links = url_clean_join(links, url=base_or_url)
        embeds = url_clean_join(embeds, url=base_or_url)

    with stats.record_burn(burn_prefix+'sha1 html', url=url):
        sha1 = 'sha1:' + hashlib.sha1(html_bytes).hexdigest()

    with stats.record_burn(burn_prefix+'facets', url=url):
        # XXX if we are using find_body_links_re we don't have any body embeds
        # in that case we might want to analyze body links instead?
        facets = facet.compute_all(html, head, body, headers, links, embeds, head_soup=head_soup, url=url)

    return links, embeds, sha1, facets


def find_html_links_re(html):
    '''
    Find the outgoing links and embeds in html, both head and body.
    This can't tell the difference between links and embeds, so we
    call them all links.

    On a 3.4ghz x86 core, runs at ~ 50 megabytes/sec.
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
    Find links in an html body, divided among links and embeds.

    On a 3.4 ghz x86 core, runs at ~ 25 megabyte/sec.
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


def split_head_body(html, url=None):
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
    m = re.search(r'<head[\s>]', html[:2000], re.I)
    if not m:
        stats.stats_sum('parser split short fail', 1)
        # well darn. try the same re as below, but with limited size
        m = re.search(r'<(?:/head>|body[\s>])', html[:50000], re.I)
        if not m:
            return '', html
        else:
            stats.stats_sum('parser split short fail save', 1)
            return html[:m.start()], html[m.end():]

    # having seen <head>, we're willing to parse for a long time
    m = re.search(r'<(?:/head>|body[\s>])', html[:1000000], re.I)
    if not m:
        stats.stats_sum('parser split long fail', 1)
        return '', html

    return html[:m.start()], html[m.end():]  # matched text is not included in either


def parse_refresh(s):
    '''
    https://www.w3.org/TR/html5/document-metadata.html#statedef-http-equiv-refresh

    See in real life and not standard-conforming, in order of popularity:
      whitespace after t before the ';'
      starting with a decimal point
      starting with a minus sign
      empty time, starts with ';'
      url= but missing the ';'
    None of these actually work in modern FF, Chrome, or Safari
    '''
    t = None
    refresh = r'\s* (\d+) (?:\.[\d\.]*)? [;,] \s* ([Uu][Rr][Ll] \s* = \s* ["\']?)? (.*)'
    m = re.match(refresh, s, re.X)
    if m:
        t, sep, url = m.groups()
        if sep and sep.endswith('"') and '"' in url:
            url = url[:url.index('"')]
        if sep and sep.endswith("'") and "'" in url:
            url = url[:url.index("'")]
    else:
        if s.isdigit():
            t = int(s)
        url = None
    return t, url


'''
Helpers to minimize how many bytes we have to html parse.
Of course, these are all dangerous, but they might be useful
if the <head> of a webpage is abnormally large
'''


def regex_out_comments(html):
    # I think whitespaace is allowed: < \s* !-- .* -- \s* > XXX
    return re.sub('<!--.*?-->', '', html, flags=re.S)


def regex_out_some_scripts(html):
    '''
    This nukes <script>...</script>, but does not nuke <script type="...
    '''
    return re.sub('<script>.*?</script>', '', html, flags=re.S)


def regex_out_all_scripts(html):
    return re.sub('<script[\s>].*?</script>', '', html, flags=re.S)
