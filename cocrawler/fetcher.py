#        ClientError
#            ClientConnectionError -- socket-related stuff
#                ClientOSError(ClientConnectionError, builtins.OSError) -- errno is set
#                ClientTimeoutError(ClientConnectionError, concurrent.futures._base.TimeoutError)
#                FingerprintMismatch -- SSL-related
#                ProxyConnectionError -- opening connection to proxy
#            ClientHttpProcessingError
#                ClientRequestError -- connection error during sending request
#                ClientResponseError -- connection error during reading respone
#        DisconnectedError
#            ClientDisconnectedError
#                WSClientDisconnectedError -- deprecated
#            ServerDisconnectedError
#        HttpProcessingError
#            BadHttpMessage -- 400
#                BadStatusLine
#                HttpBadRequest -- 400
#                InvalidHeader
#                LineTooLong
#            HttpMethodNotAllowed -- 405
#            HttpProxyError -- anything other than success starting to talk to the proxy
#            WSServerHandshakeError -- websocket-related

'''
async fetching of urls.

Assumes robots checks have already been done.

Supports proxies and server mocking.

Success returns response object (caller must release()) and response
bytes (which were already read in order to shake out all potential
errors.)

Failure returns enough details for the caller to do something smart:
503, other 5xx, DNS fail, connect timeout, error between connect and
full response, proxy failure. Plus an errorstring good enough for logging.
'''

import asyncio
import logging
import aiohttp

import time

import stats

LOGGER = logging.getLogger(__name__)

# XXX should be a policy plugin
def apply_url_policies(url, parts, config):
    headers = {}
    proxy = config['Fetcher'].get('ProxyAll')
    mock_url = None
    mock_robots = None

    test_host = config['Testing'].get('TestHostmapAll')

    if test_host:
        headers['Host'] = parts.netloc
        mock_url = parts._replace(netloc=test_host).geturl()
        mock_robots = parts.scheme + '://' + test_host + '/robots.txt'

    return headers, proxy, mock_url, mock_robots

async def fetch(url, session, headers=None, proxy=None, mock_url=None, allow_redirects=None):
    if proxy:
        proxy = aiohttp.ProxyConnector(proxy=proxy)
        # we need to preserve the existing connector config (see cocrawler.__init__)
        # XXX need to research how to do this
        raise ValueError('not yet implemented')

    t0_total_delay = time.time()

    try:
        t0 = time.time()
        response = await session.get(mock_url or url, allow_redirects=allow_redirects, headers=headers)
        # XXX special sleepy 503 handling here - soft fail
        # XXX retry handling loop here -- jsonlog count
        # XXX test with DNS error - soft fail
        # XXX serverdisconnected is a soft fail
        # XXX aiodns.error.DNSError
        # XXX equivalent to requests.exceptions.SSLerror ?? reddit.com is an example of a CDN-related SSL fail
    except aiohttp.errors.ClientError as e:
        stats.stats_sum('URL fetch ClientError exceptions', 1)
        # XXX json log something at total fail
        LOGGER.debug('fetching url %r raised %r', url, e)
        raise
    except aiohttp.errors.ServerDisconnectedError as e:
        stats.stats_sum('URL fetch ServerDisconnectedError exceptions', 1)
        # XXX json log something at total fail
        LOGGER.debug('fetching url %r raised %r', url, e)
        raise
    except Exception as e:
        stats.stats_sum('URL fetch Exception exceptions', 1)
        # XXX json log something at total fail
        LOGGER.debug('fetching url %r raised %r', url, e)
        raise

    # fully receive headers and body. XXX if we want to limit bytecount, do it here?
    body_bytes = await response.read()
    header_bytes = response.raw_headers

    stats.stats_sum('URLs fetched', 1)
    LOGGER.debug('url %r came back with status %r', url, response.status)
    stats.stats_sum('fetch http code=' + str(response.status), 1)

    apparent_elapsed = '{:.3f}'.format(time.time() - t0)

    return response, body_bytes, header_bytes, apparent_elapsed


    # checks after fetch:
    # hsts? if ssl, check strict-transport-security header, remember max-age=foo part., other stuff like includeSubDomains
    # did we receive cookies? was the security bit set?
    # fish dns for host out of tcpconnector object?
    # record everything needed for warc. all headers, for example.

def upgrade_scheme(url):
    '''
    Upgrade crawled scheme to https, if reasonable. This helps to reduce MITM attacks
    against the crawler.
    https://chromium.googlesource.com/chromium/src/net/+/master/http/transport_security_state_static.json

    Alternately, the return headers from a site might have strict-transport-security set ... a bit more
    dangerous as we'd have to respect the timeout to avoid permanently learning something that's broken

    TODO: use HTTPSEverwhere? would have to have a fallback if https failed, which it occasionally will
    '''
    return url


