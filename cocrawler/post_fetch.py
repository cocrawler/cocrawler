'''
builtin post_fetch event handler
special seed handling -- don't count redirs in depth, and add www.seed if naked seed fails (or reverse)
do this by elaborating the work unit to have an arbitrary callback

parse links and embeds, using an algorithm chosen in conf -- cocrawler.cocrawler

parent subsequently calls add_url on them -- cocrawler.cocrawler
'''

import logging
import cgi
from functools import partial
import json
import codecs

import multidict

try:
    import cchardet as chardet
except ImportError:  # pragma: no cover
    import chardet

from . import urls
from . import parse
from . import stats
from . import config
from . import seeds
from . import facet
from . import geoip

LOGGER = logging.getLogger(__name__)


# aiohttp.ClientReponse lacks this method, so...
def is_redirect(response):
    return 'Location' in response.headers and response.status in (301, 302, 303, 307, 308)


# because we're using the streaming interface we can't call resp.get_encoding()
# this is the same algo as aiohttp
def my_get_encoding(charset, body_bytes):
    detect = chardet.detect(body_bytes)
    if detect['encoding']:
        detect['encoding'] = detect['encoding'].lower()
    if detect['confidence']:
        detect['confidence'] = '{:.2f}'.format(detect['confidence'])

    for encoding in (charset, detect['encoding'], 'utf-8'):
        if encoding:
            try:
                codecs.lookup(encoding)
                break
            except LookupError:
                pass
    else:
        encoding = None

    return encoding, detect


def my_decode(body_bytes, encoding, detect):
    for charset in encoding, detect['encoding']:
        if not charset:
            # encoding or detect may be None
            continue
        try:
            body = body_bytes.decode(encoding=charset)
            break
        except UnicodeDecodeError:
            # if we truncated the body, we could have caused the error:
            # UnicodeDecodeError: 'utf-8' codec can't decode byte 0xd9 in position 15: unexpected end of data
            # or encoding could be wrong, or the page could be defective
            pass
    else:
        body = body_bytes.decode(encoding='utf-8', errors='replace')
        charset = 'utf-8 replace'
    return body, charset


def charset_log(json_log, charset, detect, charset_used):
    '''
    Log details, but only if interesting
    '''
    interesting = False

    if ' replace' in charset_used:
        interesting = True
    elif not charset:
        interesting = True
        stats.stats_sum('cchardet used', 1)
    elif charset != charset_used:
        interesting = True
        stats.stats_sum('cchardet used', 1)

    if interesting:
        json_log['cchardet_charset'] = detect['encoding']
        json_log['cchardet_confidence'] = detect['confidence']

    json_log['charset'] = charset_used
    stats.stats_sum('charset='+charset_used, 1)


def minimal_facet_me(resp_headers, url, host_geoip, kind, t, crawler, seed_host=None, location=None):
    if not crawler.facetlogfd:
        return
    facets = facet.compute_all('', '', '', resp_headers, [], [], url=url)
    geoip.add_facets(facets, host_geoip)
    if not isinstance(url, str):
        url = url.url

    facet_log = {'url': url, 'facets': facets, 'kind': kind, 'time': t}
    if seed_host:
        facet_log['seed_host'] = seed_host
    if location:  # redirect
        facet_log['location'] = location

    print(json.dumps(facet_log, sort_keys=True), file=crawler.facetlogfd)


'''
If we're robots blocked, the only 200 we're ever going to get is
for robots.txt. So, facet it.
'''


def post_robots_txt(f, url, host_geoip, t, crawler, seed_host=None):
    resp_headers = f.response.headers
    minimal_facet_me(resp_headers, url, host_geoip, 'robots.txt', t, crawler, seed_host=seed_host)


'''
Study redirs at the host level to see if we're systematically getting
redirs from bare hostname to www or http to https, so we can do that
transformation in advance of the fetch.

Try to discover things that look like unknown url shorteners. Known
url shorteners should be treated as high-priority so that we can
capture the real underlying url before it has time to change, or for
the url shortener to go out of business.
'''


def handle_redirect(f, url, ridealong, priority, host_geoip, json_log, crawler, seed_host=None):
    resp_headers = f.response.headers

    location = resp_headers.get('location')
    if location is None:
        seeds.fail(ridealong, crawler)
        LOGGER.info('%d redirect for %s has no Location: header', f.response.status, url.url)
        raise ValueError(url.url + ' sent a redirect with no Location: header')
    next_url = urls.URL(location, urljoin=url)

    minimal_facet_me(resp_headers, url, host_geoip, 'redir', json_log['time'], crawler,
                     seed_host=seed_host, location=next_url.url)

    ridealong['url'] = next_url

    redir_kind = urls.special_redirect(url, next_url)
    samesurt = url.surt == next_url.surt

    if 'seed' in ridealong:
        prefix = 'redirect seed'
    else:
        prefix = 'redirect'
    if redir_kind is not None:
        stats.stats_sum(prefix+' '+redir_kind, 1)
    else:
        stats.stats_sum(prefix+' non-special', 1)

    queue_next = True

    if redir_kind is None:
        if samesurt:
            LOGGER.info('Whoops, %s is samesurt but not a special_redirect: %s to %s, location %s',
                        prefix, url.url, next_url.url, location)
    elif redir_kind == 'same':
        LOGGER.info('attempted redirect to myself: %s to %s, location was %s', url.url, next_url.url, location)
        if 'Set-Cookie' not in resp_headers:
            LOGGER.info(prefix+' to myself and had no cookies.')
            stats.stats_sum(prefix+' same with set-cookie', 1)
        else:
            stats.stats_sum(prefix+' same without set-cookie', 1)
        seeds.fail(ridealong, crawler)
        queue_next = False
    else:
        LOGGER.info('special redirect of type %s for url %s', redir_kind, url.url)
        # XXX push this info onto a last-k for the host
        # to be used pre-fetch to mutate urls we think will redir

    priority += 1

    if samesurt and redir_kind != 'same':
        ridealong['skip_seen_url'] = True

    if 'freeredirs' in ridealong:
        priority -= 1
        json_log['freeredirs'] = ridealong['freeredirs']
        ridealong['freeredirs'] -= 1
        if ridealong['freeredirs'] == 0:
            del ridealong['freeredirs']
    ridealong['priority'] = priority

    if queue_next:
        crawler.add_url(priority, ridealong)

    json_log['redirect'] = next_url.url
    json_log['location'] = location
    if redir_kind is not None:
        json_log['redir_kind'] = redir_kind
    if queue_next:
        json_log['found_new_links'] = 1
    else:
        json_log['found_new_links'] = 0

    # after we return, json_log will get logged


async def post_200(f, url, priority, host_geoip, seed_host, json_log, crawler):

    if crawler.warcwriter is not None:  # needs to use the same algo as post_dns for choosing what to warc
        # XXX insert the digest we already computed, instead of computing it again?
        crawler.warcwriter.write_request_response_pair(url.url, f.req_headers,
                                                       f.response.raw_headers, f.is_truncated, f.body_bytes)

    resp_headers = f.response.headers
    content_type = resp_headers.get('content-type', '')
    # sometimes content_type comes back multiline. whack it with a wrench.
    content_type = content_type.replace('\r', '\n').partition('\n')[0]
    content_type, options = cgi.parse_header(content_type)

    json_log['content_type'] = content_type
    stats.stats_sum('content-type=' + content_type, 1)
    if 'charset' in options:
        charset = options['charset'].lower()
        json_log['content_type_charset'] = charset
        stats.stats_sum('content-type-charset=' + charset, 1)
    else:
        charset = None
        stats.stats_sum('content-type-charset=' + 'not specified', 1)

    html_types = set(('text/html', '', 'application/xml+html'))

    if content_type in html_types:
        with stats.record_burn('response body get_encoding', url=url):
            encoding, detect = my_get_encoding(charset, f.body_bytes)
        with stats.record_burn('response body decode', url=url):
            body, charset_used = my_decode(f.body_bytes, encoding, detect)

        charset_log(json_log, charset, detect, charset_used)

        if len(body) > int(config.read('Multiprocess', 'ParseInBurnerSize')):
            stats.stats_sum('parser in burner thread', 1)
            # headers is a multidict.CIMultiDictProxy case-blind dict
            # and the Proxy form of it doesn't pickle, so convert to one that does
            resp_headers = multidict.CIMultiDict(resp_headers)
            try:
                links, embeds, sha1, facets = await crawler.burner.burn(
                    partial(parse.do_burner_work_html, body, f.body_bytes, resp_headers,
                            burn_prefix='burner ', url=url),
                    url=url)
            except ValueError as e:  # if it pukes, we get back no values
                stats.stats_sum('parser raised', 1)
                LOGGER.info('parser raised %r', e)
                # XXX jsonlog
                return
        else:
            stats.stats_sum('parser in main thread', 1)
            try:
                # no coroutine state because this is a burn, not an await
                links, embeds, sha1, facets = parse.do_burner_work_html(
                    body, f.body_bytes, resp_headers, burn_prefix='main ', url=url)
            except ValueError:  # if it pukes, ..
                stats.stats_sum('parser raised', 1)
                # XXX jsonlog
                return
        json_log['checksum'] = sha1

        geoip.add_facets(facets, host_geoip)

        facet_log = {'url': url.url, 'facets': facets, 'kind': 'get'}
        facet_log['checksum'] = sha1
        facet_log['time'] = json_log['time']
        if seed_host:
            facet_log['seed_host'] = seed_host

        if crawler.facetlogfd:
            print(json.dumps(facet_log, sort_keys=True), file=crawler.facetlogfd)

        LOGGER.debug('parsing content of url %r returned %d links, %d embeds, %d facets',
                     url.url, len(links), len(embeds), len(facets))
        json_log['found_links'] = len(links) + len(embeds)
        stats.stats_max('max urls found on a page', len(links) + len(embeds))

        max_tries = config.read('Crawl', 'MaxTries')

        new_links = 0
        for u in links:
            ridealong = {'url': u, 'priority': priority+1, 'retries_left': max_tries}
            if crawler.add_url(priority + 1, ridealong):
                new_links += 1
        for u in embeds:
            ridealong = {'url': u, 'priority': priority-1, 'retries_left': max_tries}
            if crawler.add_url(priority - 1, ridealong):
                new_links += 1

        if new_links:
            json_log['found_new_links'] = new_links

        # XXX process meta-http-equiv-refresh

        # XXX plugin for links and new links - post to Kafka, etc
        # neah stick that in add_url!

        # actual jsonlog is emitted after the return


def post_dns(dns, expires, url, crawler):
    if crawler.warcwriter is not None:  # needs to use the same algo as post_200 for choosing what to warc
        crawler.warcwriter.write_dns(dns, expires, url)
