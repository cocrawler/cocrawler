'''
async fetching of urls.

Assumes robots checks have already been done.

Supports server mocking; proxies are not yet implemented.

Success returns response object and response bytes (which were already
read in order to shake out all potential exceptions.)

Failure returns enough details for the caller to do something smart:
503, other 5xx, DNS fail, connect timeout, error between connect and
full response, proxy failure. Plus an errorstring good enough for logging.

'''

import time
import traceback
from collections import namedtuple
import ssl

import asyncio
import logging
import aiohttp
import aiodns

from . import stats
from . import dns

LOGGER = logging.getLogger(__name__)


# XXX should be a policy plugin
def apply_url_policies(url, config):
    headers = {}
    proxy = None
    mock_url = None
    mock_robots = None

    test_host = config['Testing'].get('TestHostmapAll')
    if test_host:
        headers['Host'] = url.urlparse.netloc
        mock_url = url.urlparse._replace(netloc=test_host).geturl()
        mock_robots = url.urlparse.scheme + '://' + test_host + '/robots.txt'

    # XXX set header Upgrade-Insecure-Requests: 1 ??

    return headers, proxy, mock_url, mock_robots


FetcherResponse = namedtuple('FetcherResponse', ['response', 'body_bytes',
                                                 't_first_byte', 't_last_byte', 'last_exception'])


async def fetch(url, session, config,
                headers=None, proxy=None, mock_url=None, allow_redirects=None, stats_me=True):
    maxsubtries = int(config['Crawl']['MaxSubTries'])
    pagetimeout = float(config['Crawl']['PageTimeout'])
    retrytimeout = float(config['Crawl']['RetryTimeout'])

    if proxy:  # pragma: no cover
        proxy = aiohttp.ProxyConnector(proxy=proxy)
        # XXX we need to preserve the existing connector config (see cocrawler.__init__ for conn_kwargs)
        # XXX we should rotate proxies every fetch in case some are borked
        # XXX use proxy history to decide not to use some
        raise ValueError('not yet implemented')

    subtries = 0
    last_exception = None
    response = None
    iplist = []

    while subtries < maxsubtries:
        subtries += 1
        try:
            t0 = time.time()
            last_exception = None

            if len(iplist) == 0:
                iplist = await dns.prefetch_dns(url, mock_url, session)

            with stats.coroutine_state('fetcher fetching'):
                with aiohttp.Timeout(pagetimeout):
                    response = None
                    response = await session.get(mock_url or url.url,
                                                 allow_redirects=allow_redirects,
                                                 headers=headers)
                    t_first_byte = '{:.3f}'.format(time.time() - t0)
                    if stats_me:
                        stats.record_a_latency('fetcher fetching', t0, url=url)

                    # XXX json_log tries?
                    # reddit.com is an example of a CDN-related SSL fail
                    # XXX when we retry, if local_addr was a list, switch to a different source IP
                    #   (change out the TCPConnector)

                    # fully receive headers and body.
                    # XXX if we want to limit bytecount, do it here?
                    body_bytes = await response.read()
                    t_last_byte = '{:.3f}'.format(time.time() - t0)

            if len(iplist) == 0:
                LOGGER.info('surprised that no-ip-address fetch of %s succeeded', url.urlparse.netloc)

            # break only if we succeeded. 5xx = retry, exception = retry
            if response.status < 500:
                break

            LOGGER.info('will retry a %d for %s', response.status, url.url)

        except (aiohttp.ClientError, aiodns.error.DNSError, asyncio.TimeoutError, RuntimeError) as e:
            last_exception = repr(e)
            LOGGER.debug('we sub-failed once, url is %s, exception is %s', url.url, last_exception)
        except (ssl.CertificateError, ValueError, AttributeError) as e:
            # ValueError = 'Can redirect only to http or https'
            #  (XXX BUG in aiohttp: not case blind comparison - no bug opened yet)
            # Value Error Location: https:/// 'Host could not be detected'
            # AttributeError: 'NoneType' object has no attribute 'errno' - fires when CNAME has no A
            # XXX ssl.CertificateErrors are not yet propagating -- missing a Raise
            # https://github.com/python/asyncio/issues/404
            last_exception = repr(e)
            LOGGER.debug('we choose to fail, url is %s, exception is %s', url.url, last_exception)
            subtries += maxsubtries
            continue  # fall out of the loop as if we exhausted subtries
        except asyncio.CancelledError:
            raise
        except Exception as e:
            last_exception = repr(e)
            traceback.print_exc()
            LOGGER.info('we sub-failed once: url is %s, exception is %s',
                        url.url, last_exception)

        # treat all 5xx somewhat similar to a 503: slow down and retry
        # also doing this slow down for any exception
        # XXX record 5xx so that everyone else slows down, too (politeness)
        with stats.coroutine_state('fetcher retry sleep'):
            await asyncio.sleep(retrytimeout)

    else:
        if last_exception:
            LOGGER.debug('we failed, the last exception is %s', last_exception)
            return FetcherResponse(None, None, None, None, last_exception)
        # fall through for the case of response.status >= 500

    stats.stats_sum('fetch bytes', len(body_bytes) + len(response.raw_headers))

    if stats_me:
        stats.stats_sum('fetch URLs', 1)
        stats.stats_sum('fetch http code=' + str(response.status), 1)

    # checks after fetch:
    # hsts? if ssl, check strict-transport-security header,
    #   remember max-age=foo part., other stuff like includeSubDomains
    # did we receive cookies? was the security bit set?
    # generate warc here? both normal and robots fetches go through here.

    return FetcherResponse(response, body_bytes, t_first_byte, t_last_byte, None)


def upgrade_scheme(url):
    '''
    Upgrade crawled scheme to https, if reasonable. This helps to reduce MITM attacks against the crawler.

    https://chromium.googlesource.com/chromium/src/net/+/master/http/transport_security_state_static.json

    Alternately, the return headers from a site might have strict-transport-security set ... a bit more
    dangerous as we'd have to respect the timeout to avoid permanently learning something that's broken

    TODO: use HTTPSEverwhere? would have to have a fallback if https failed, which it occasionally will
    '''
    return url
