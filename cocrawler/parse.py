'''
Parse links in html and css pages.
'''

import logging
import re
import hashlib
import urllib.parse
from functools import partial
import multidict
import html

from bs4 import BeautifulSoup

from . import stats
from .urls import URL
from . import facet
from . import config

LOGGER = logging.getLogger(__name__)


async def do_parser(body, body_bytes, resp_headers, url, crawler):
    if len(body) > int(config.read('Multiprocess', 'ParseInBurnerSize')):
        stats.stats_sum('parser in burner thread', 1)
        # headers is a multidict.CIMultiDictProxy case-blind dict
        # and the Proxy form of it doesn't pickle, so convert to one that does
        resp_headers = multidict.CIMultiDict(resp_headers)
        links, embeds, sha1, facets, base = await crawler.burner.burn(
            partial(do_burner_work_html, body, body_bytes, resp_headers,
                    burn_prefix='burner ', url=url),
            url=url)
    else:
        stats.stats_sum('parser in main thread', 1)
        # no coroutine state because this is a burn, not an await
        links, embeds, sha1, facets, base = do_burner_work_html(
            body, body_bytes, resp_headers, burn_prefix='main ', url=url)

    return links, embeds, sha1, facets, base


def do_burner_work_html(html, html_bytes, headers, burn_prefix='', url=None):
    stats.stats_sum('parser html bytes', len(html_bytes))

    # This embodies a minimal parsing policy; it needs to be made pluggable/configurable
    #  split head/body
    #  soup the head so we can accurately get base and other details
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
            stats.stats_sum('head soup exception '+str(e), 1)
            raise

    base = head_soup.find('base') or {}
    base = base.get('href')
    if base:
        # base can be relative, e.g. 'subdir/' or '.'
        base = urllib.parse.urljoin(url.url, base)
    base_or_url = base or url

    with stats.record_burn(burn_prefix+'find_head_links_soup', url=url):
        links, embeds = find_head_links_soup(head_soup)

    with stats.record_burn(burn_prefix+'find_body_links_re', url=url):
        lbody, ebody = find_body_links_re(body)
        links += lbody
        embeds += ebody

    embeds = clean_link_objects(embeds, ('javascript:', 'data:'))
    links = clean_link_objects(links, ('javascript:',))

    with stats.record_burn(burn_prefix+'url_clean_join', url=url):
        links = url_clean_join(links, url=base_or_url)
        embeds = url_clean_join(embeds, url=base_or_url)

    with stats.record_burn(burn_prefix+'sha1 html', url=url):
        sha1 = 'sha1:' + hashlib.sha1(html_bytes).hexdigest()

    with stats.record_burn(burn_prefix+'facets', url=url):
        facets = facet.compute_all(html, head, body, headers, links, embeds, head_soup=head_soup, url=url)

    links = collapse_links(links)
    embeds = collapse_links(embeds)

    return links, embeds, sha1, facets, base


def collapse_links(links):
    ret = []
    for link in links:
        l = link.get('href')
        if not l:
            l = link.get('src')
        if l:
            ret.append(l)
    return ret


def clean_link_objects(link_objects, schemes):
    '''
    Drop all elements of the link_objects that are in schemes.
    '''
    schemes = tuple(schemes)
    ret = []
    for link_object in link_objects:
        if link_object is None:
            continue
        u = link_object.get('href') or link_object.get('src')
        if u and u.startswith(schemes):
            continue
        ret.append(link_object)
    return ret


def find_html_links_re(html):
    '''
    Find the outgoing links and embeds in html, both head and body.
    This can't tell the difference between links and embeds, so we
    call them all links.

    On a 3.4ghz x86 core, runs at ~ 50 megabytes/sec.
    '''
    stats.stats_sum('html_links_re parser bytes', len(html))

    delims = set(
        [m[1] for m in re.findall(r'''\s(?:href|src)\s{,3}=\s{,3}(?P<delim>['"])(.*?)(?P=delim)''', html, re.I | re.S)]
    )
    no_delims = set(re.findall(r'''\s(?:href|src)\s{,3}=\s{,3}([^\s'"<>]+)''', html, re.I))

    links = delims.union(no_delims)

    links = [{'href': h} for h in links]
    return links, []


def find_body_links_re(body):
    '''
    Find links in an html body, divided among links and embeds.

    On a 3.4 ghz x86 core, runs at ~ 25 megabyte/sec.
    '''
    stats.stats_sum('body_links_re parser bytes', len(body))

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

    embeds = [{'src': s} for s in embeds]
    links = [{'href': h} for h in links]

    return links, embeds


def find_body_links_anchors_re(body):
    '''
    Find links and anchors in an html body, divided among links and embeds.

    On a 3.4 ghz x86 core, runs at ~ NN megabyte/sec.
    '''
    stats.stats_sum('body_links_anchors_re parser bytes', len(body))

    embeds_delims = set(
        [m[1] for m in re.findall(r'''\ssrc\s{,3}=\s{,3}(?P<delim>['"])(.*?)(?P=delim)''', body, re.I | re.S | re.X)]
    )
    embeds_no_delims = set(re.findall(r'''\ssrc\s{,3}=\s{,3}([^\s'"<>]+)''', body, re.I | re.X))
    embeds = embeds_delims.union(embeds_no_delims)

    links_delims = set()
    for m in re.finditer(r'''\shref\s{,3}=\s{,3}(?P<delim>['"])(.*?)(?P=delim) [^>]{,400} >''', body, re.I | re.S | re.X):
        delim = m.group(1)
        href = m.group(2)

        if delim in href:
            # this happens when the size above isn't big enough.
            href = href.split(delim, 1)[0]
            stats.stats_sum('body_links_anchors_re parser extra delim split needed', 1)

        if href.startswith('#'):
            continue
        end = m.end(0)
        anchor = body[end:]
        mm = re.match(r'(.{,101}?)</a>', anchor, re.I | re.S)
        if mm:
            anchor = mm.group(1)
        else:
            anchor = anchor.split('<', 1)[0]
        links_delims.add((href, anchor))

    links_no_delims = set()
    for m in re.finditer(r'''\shref\s{,3}=\s{,3}([^'">\s]+) [^>]{,200} >''', body, re.I | re.S | re.X):
        href = m.group(1)
        if href == '#':
            continue
        end = m.end(0)
        anchor = body[end:]
        mm = re.match(r'(.{,101}?)</a>', anchor, re.I | re.S)
        if mm:
            anchor = mm.group(1)
        else:
            anchor = anchor.split('<', 1)[0]
        links_no_delims.add((href, anchor))

    links = links_delims.union(links_no_delims)

    embeds = [{'src': s} for s in embeds]
    links = [dict((('href', h[0]), *trim_anchor(h[1]))) for h in links]

    return links, embeds


def find_css_links_re(css):
    '''
    Finds the links embedded in css files
    '''
    stats.stats_sum('css_links_re parser bytes', len(css))

    embeds_delims = set(
        [m[1] for m in re.findall(r'''\surl\(\s?(?P<delim>['"])(.*?)(?P=delim)''', css, re.I | re.S)]
    )
    embeds_no_delims = set(re.findall(r'''\surl\(\s?([^\s'"<>()]+)''', css, re.I))

    return [], list(embeds_delims.union(embeds_no_delims))


def find_head_links_soup(head_soup):
    embeds = []
    for tag in head_soup.find_all(src=True):
        lo = build_link_object(tag)
        if lo:
            embeds.append(lo)
    for tag in head_soup.find_all(href=True):
        lo = build_link_object(tag)
        if lo:
            embeds.append(lo)
    return [], embeds


def trim_anchor(anchor):
    ret = []
    # pull out <img alt=""> here?
    anchor = re.sub(r'<.*?>', '', anchor, re.S).strip()
    anchor = html.unescape(anchor)

    if len(anchor) > 100:
        anchor = anchor[:100]
        ret.append(('anchor_truncated', True))
    if anchor:
        ret.append(('anchor', anchor))
    return ret


def build_link_object(tag):
    ret = {'tag': tag.name}

    if tag.get('href'):
        ret['href'] = tag.get('href')
    if tag.get('src'):
        ret['src'] = tag.get('src')
    if 'href' not in ret and 'src' not in ret:
        # href or src was present but false
        return

    if tag.name == 'a':
        try:
            parts = tag.itertext(with_tail=False)
        except TypeError:
            parts = None
        if parts:
            anchor = ' '.join(parts)
            anchor = re.sub(r'\s+', ' ', anchor).strip()
            ret.update(*trim_anchor(anchor))
        if tag.get('target'):
            ret['target'] = tag.get('target')

    if tag.name == 'iframe':
        if tag.get('name'):
            ret['name'] = tag.get('name')

    return ret


def find_body_links_soup(body_soup):
    embeds = []
    links = []
    for tag in body_soup.find_all(src=True):
        if tag.name == 'iframe':
            lo = build_link_object(tag)
            if lo:
                links.append(lo)
        else:
            lo = build_link_object(tag)
            if lo:
                embeds.append(lo)
    for tag in body_soup.find_all(href=True):
        if tag.name == 'link':
            rel = tag.get('rel', [None])[0]
            if rel == 'stylesheet':
                lo = build_link_object(tag)
                if lo:
                    embeds.append(lo)
            else:
                pass  # discard other body-ok like 'prefetch'
        else:
            lo = build_link_object(tag)
            if lo:
                links.append(lo)
    return links, embeds


def url_clean_join(links, url=None):
    ret = []
    for link in links:
        # you can have both, for example <link> tags occasionally incorectly have both in the wild
        if 'href' in link:
            link['href'] = URL(link['href'], urljoin=url)
        if 'src' in link:
            link['src'] = URL(link['src'], urljoin=url)
        ret.append(link)
    return ret


def url_dedup(link_objects):
    ret = []
    dedup = set()
    for link_object in link_objects:
        link = link_object.get('href') or link_object.get('src')
        if link:
            if link in dedup:
                continue
            dedup.add(link)
            ret.append(link_object)
    return ret


def report():
    # XXX fix these names
    # XXX how does this get just the burner thread? use the prefix
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

    It's legal for webpages to leave off <head> and <body>; the HTML5
    standard requires browsers to figure it out based on the html
    tags. We can't do that efficiently, so we punt for such webpages,
    and return the entire page as body.
    '''

    # heuristic: if there's a <head> tag at all, it's early in the document
    m = re.search(r'<head[\s>]', html[:2000], re.I)
    if not m:
        stats.stats_sum('parser split short fail', 1)
        # well darn. try the same re as below, but with limited size
        m = re.search(r'<(?:/head>|body[\s>])', html[:50000], re.I)
        if m:
            stats.stats_sum('parser split short fail save', 1)
            return html[:m.start()], html[m.end():]
        else:
            return '', html

    # having seen <head>, we're willing to parse for a long time for </head or <body
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
        try:
            t = int(t)
        except ValueError:
            t = None
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
    # I think whitespace is allowed: < \s* !-- .* -- \s* > XXX
    return re.sub(r'<!--.*?-->', '', html, flags=re.S)


def regex_out_some_scripts(html):
    '''
    This nukes <script>...</script>, but does not nuke <script type="...
    '''
    return re.sub(r'<script>.*?</script>', '', html, flags=re.S)


def regex_out_all_scripts(html):
    return re.sub(r'<script[\s>].*?</script>', '', html, flags=re.S)
