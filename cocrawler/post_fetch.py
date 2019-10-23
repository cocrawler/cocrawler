'''
builtin post_fetch event handler
special seed handling -- don't count redirs in depth, and add www.seed if naked seed fails (or reverse)
do this by elaborating the work unit to have an arbitrary callback

parse links and embeds, using an algorithm chosen in conf -- cocrawler.cocrawler

parent subsequently calls add_url on them -- cocrawler.cocrawler
'''

import logging
import json
import time

from bs4 import BeautifulSoup

from . import urls
from . import parse
from . import stats
from . import config
from . import seeds
from . import facet
from . import geoip
from . import content

LOGGER = logging.getLogger(__name__)


# aiohttp.ClientReponse lacks this method, so...
def is_redirect(response):
    return response.status in {301, 302, 303, 307, 308}


def should_retry(f):
    if f.last_exception is not None:
        return True
    if f.response.status >= 500:
        return True
    if f.response.status in {403, 429}:
        return True


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

    head_soup = BeautifulSoup('', 'lxml')
    facets = facet.compute_all('', '', '', resp_headers, [], [], head_soup=head_soup, url=url)
    geoip.add_facets(facets, host_geoip)
    if not isinstance(url, str):
        url = url.url

    facet_log = {'url': url, 'facets': facets, 'kind': kind, 'time': t}
    if seed_host:
        facet_log['seed_host'] = seed_host
    if location:  # redirect
        facet_log['location'] = location

    print(json.dumps(facet_log, sort_keys=True), file=crawler.facetlogfd)


def post_robots_txt(f, url, host_geoip, t, crawler, seed_host=None):
    '''
    If we're robots blocked, the only 200 we're ever going to get is
    for robots.txt. So, facet it.

    But, if we redirected to a non-robots.txt, usually /, don't. That's an
    invalid robots.txt and we'll crawl the site.
    '''
    if url.endswith('/'):
        return

    resp_headers = f.response.headers
    minimal_facet_me(resp_headers, url, host_geoip, 'robots.txt', t, crawler, seed_host=seed_host)

    if crawler.warcwriter is not None:  # needs to use the same algo as post_2xx for choosing what to warc
        crawler.warcwriter.write_request_response_pair(url, f.ip, f.req_headers,
                                                       f.response.raw_headers, f.is_truncated, f.body_bytes,
                                                       decompressed=False)


'''
Study redirs at the host level to see if we're systematically getting
redirs from bare hostname to www or http to https, so we can do that
transformation in advance of the fetch.

Try to discover things that look like unknown url shorteners. Known
url shorteners should be treated as high-priority so that we can
capture the real underlying url before it has time to change, or for
the url shortener to go out of business.
'''


def handle_redirect(f, url, ridealong, priority, host_geoip, json_log, crawler, rand=None):
    resp_headers = f.response.headers
    seed_host = ridealong.get('seed_host')

    location = resp_headers.get('location')
    if location is None:
        seeds.fail(ridealong, crawler, json_log)
        LOGGER.info('%d redirect for %s has no Location: header', f.response.status, url.url)
        next_url = None
        kwargs = {}
    else:
        next_url = urls.URL(location, urljoin=url)
        kwargs = {'location': next_url.url}

    minimal_facet_me(resp_headers, url, host_geoip, 'redir', json_log['time'], crawler,
                     seed_host=seed_host, **kwargs)

    if next_url is None:
        seeds.fail(ridealong, crawler, json_log)
        return

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
        seeds.fail(ridealong, crawler, json_log)
        queue_next = False
    else:
        LOGGER.debug('special redirect of type %s for url %s', redir_kind, url.url)
        # XXX push this info onto a last-k for the host
        # to be used pre-fetch to mutate urls we think will redir

    priority += 1

    if samesurt and redir_kind != 'same':
        ridealong['skip_crawled'] = True

    if 'freeredirs' in ridealong:
        priority -= 1
        json_log['freeredirs'] = ridealong['freeredirs']
        ridealong['freeredirs'] -= 1
        if ridealong['freeredirs'] == 0:
            del ridealong['freeredirs']
    ridealong['priority'] = priority

    if queue_next:
        crawler.add_url(priority, ridealong, rand=rand)

    json_log['redirect'] = next_url.url
    json_log['location'] = location
    if redir_kind is not None:
        json_log['redir_kind'] = redir_kind
    if queue_next:
        json_log['found_new_links'] = 1
    else:
        json_log['found_new_links'] = 0

    # after we return, json_log will get logged


async def post_2xx(f, url, ridealong, priority, host_geoip, json_log, crawler):

    if crawler.warcwriter is not None:
        # needs to use the same algo as post_dns for choosing what to warc
        # insert the digest instead of computing it twice? see sha1 below
        # we delayed decompression so that we could warc the compressed body
        crawler.warcwriter.write_request_response_pair(url.url, f.ip, f.req_headers,
                                                       f.response.raw_headers, f.is_truncated, f.body_bytes,
                                                       decompressed=False)

    resp_headers = f.response.headers
    content_type, content_encoding, charset = content.parse_headers(resp_headers, json_log)

    html_types = set(('text/html', '', 'application/xhtml+xml'))
    html_types.add('')  # no content type
    html_types.add('*/*')  # mildly common in the wild, whatwg says we should sniff in this case

    if content_type not in html_types:
        # XXX sniff the type https://mimesniff.spec.whatwg.org/
        json_log['comment'] = 'not an html content type'
        #json_log['checksum'] = sha1  # XXX would like to log this
    else:
        if content_encoding != 'identity':
            with stats.record_burn('response body decompress', url=url):
                body_bytes = content.decompress(f.body_bytes, content_encoding, url=url)
            stats.stats_sum('response body decompress bytes', len(body_bytes))
        else:
            body_bytes = f.body_bytes

        with stats.record_burn('response body get_charset', url=url):
            charset, detect = content.my_get_charset(charset, body_bytes)
        with stats.record_burn('response body decode', url=url):
            body, charset_used = content.my_decode(body_bytes, charset, detect)

        charset_log(json_log, charset, detect, charset_used)

        try:
            links, embeds, sha1, facets, base = await parse.do_parser(body, body_bytes, resp_headers, url, crawler)
        except ValueError as e:
            stats.stats_sum('parser raised', 1)
            LOGGER.info('parser raised %r', e)
            # XXX jsonlog
            return

        json_log['checksum'] = sha1

        geoip.add_facets(facets, host_geoip)

        facet_log = {'url': url.url, 'facets': facets, 'kind': 'get'}
        if base is not None:
            facet_log['base'] = base
        facet_log['checksum'] = sha1
        facet_log['time'] = json_log['time']

        seed_host = ridealong.get('seed_host')
        if seed_host:
            facet_log['seed_host'] = seed_host

        if crawler.facetlogfd:
            print(json.dumps(facet_log, sort_keys=True), file=crawler.facetlogfd)

        LOGGER.debug('parsing content of url %r returned %d links, %d embeds, %d facets',
                     url.url, len(links), len(embeds), len(facets))
        json_log['found_links'] = len(links) + len(embeds)
        stats.stats_max('max urls found on a page', len(links) + len(embeds))

        max_tries = config.read('Crawl', 'MaxTries')
        queue_embeds = config.read('Crawl', 'QueueEmbeds')

        new_links = 0
        ridealong_skeleton = {'priority': priority+1, 'retries_left': max_tries}
        if seed_host:
            ridealong_skeleton['seed_host'] = seed_host
        for u in links:
            ridealong = {'url': u}
            ridealong.update(ridealong_skeleton)
            if crawler.add_url(priority + 1, ridealong):
                new_links += 1
        if queue_embeds:
            for u in embeds:
                ridealong = {'url': u}
                ridealong.update(ridealong_skeleton)
                if crawler.add_url(priority - 1, ridealong):
                    new_links += 1

        if new_links:
            json_log['found_new_links'] = new_links

        # XXX process meta-http-equiv-refresh

        # XXX plugin for links and new links - post to Kafka, etc
        # neah stick that in add_url!

        # actual jsonlog is emitted after the return


def post_dns(dns, expires, url, crawler):
    if crawler.warcwriter is not None:  # needs to use the same algo as post_2xx for choosing what to warc
        now = time.time()
        crawler.warcwriter.write_dns(dns, expires-now, url)
