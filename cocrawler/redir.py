'''
Handle processing of redirects in fetches.

Study redirs at the host level to see if we're systematically getting
redirs from bare hostname to www or http to https, so we can do that
transformation in advance of the fetch.

Try to discover things that look like unknown url shorteners. Known
url shorteners should be treated as high-priority so that we can
capture the real underlying url before it has time to change, or for
the url shortener to go out of business.
'''

import logging

from . import urls
from . import stats

LOGGER = logging.getLogger(__name__)


# aiohttp.ClientReponse lacks this method, so...
def is_redirect(response):
    return response.status in (300, 301, 302, 303, 307)


def handle_redirect(response, url, ridealong, priority, json_log, config, crawler):
    resp_headers = response.headers
    location = resp_headers.get('location')
    if location is None:
        LOGGER.info('%d redirect for %s has no Location: header', f.response.status, url.url)
        # XXX this raise causes "ERROR:asyncio:Task exception was never retrieved"
        raise ValueError(url.url + ' sent a redirect with no Location: header')
    next_url = urls.URL(location, urljoin=url)

    kind = urls.special_redirect(url, next_url)
    if kind is not None:
        if 'seed' in ridealong:
            prefix = 'redirect seed'
        else:
            prefix = 'redirect'
        stats.stats_sum(prefix+' '+kind, 1)

    # XXX need to handle 'samesurt' case
    if kind is None:
        pass
    elif kind == 'same':
        LOGGER.info('attempted redirect to myself: %s to %s', url.url, next_url.url)
        if 'Set-Cookie' not in resp_headers:
            LOGGER.info('redirect to myself had no cookies.')
            # XXX try swapping www/not-www? or use a non-crawler UA.
            # looks like some hosts have extra defenses on their redir servers!
        else:
            # XXX we should use a cookie jar with this domain?
            pass
        # fall through; will fail seen-url test in addurl
    else:
        #LOGGER.info('special redirect of type %s for url %s', kind, url.url)
        # XXX push this info onto a last-k for the host
        # to be used pre-fetch to mutate urls we think will redir
        pass

    priority += 1
    json_log['redirect'] = next_url.url

    kwargs = {}
    if 'seed' in ridealong:
        if 'seedredirs' in ridealong:
            ridealong['seedredirs'] += 1
        else:
            ridealong['seedredirs'] = 1
        if ridealong['seedredirs'] > config['Seeds'].get('SeedRedirsCount', 0):
            del ridealong['seed']
            del ridealong['seedredirs']
        else:
            kwargs['seed'] = ridealong['seed']
            kwargs['seedredirs'] = ridealong['seedredirs']
            if config['Seeds'].get('SeedRedirsFree'):
                priority -= 1
            json_log['seedredirs'] = ridealong['seedredirs']

    if crawler.add_url(priority, next_url, **kwargs):
        json_log['found_new_links'] = 1
