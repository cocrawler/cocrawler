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

import time
import traceback
import urllib
from collections import namedtuple

import asyncio
import logging
import aiohttp
import aiodns

import stats

LOGGER = logging.getLogger(__name__)

# XXX should be a policy plugin
def apply_url_policies(url, parts, config):
    headers = {}
    proxy = None
    mock_url = None
    mock_robots = None

    test_host = config['Testing'].get('TestHostmapAll')
    if test_host and test_host != 'n': # why don't booleans in YAML work?
        headers['Host'] = parts.netloc
        mock_url = parts._replace(netloc=test_host).geturl()
        mock_robots = parts.scheme + '://' + test_host + '/robots.txt'

    return headers, proxy, mock_url, mock_robots

async def prefetch_dns(parts, mock_url, session):
    if mock_url is None:
        netlocparts = parts.netloc.split(':', maxsplit=1)
    else:
        mockurlparts = urllib.parse.urlparse(mock_url)
        netlocparts = mockurlparts.netloc.split(':', maxsplit=1)
    host = netlocparts[0]
    try:
        port = int(netlocparts[1])
    except IndexError:
        port = 80

    answer = None
    iplist = []

    if (host, port) not in session.connector.cached_hosts:
        with stats.coroutine_state('fetcher DNS lookup'):
            # if this raises an exception, it's caught in the caller
            answer = await session.connector._resolve_host(host, port)
    else:
        answer = session.connector.cached_hosts[(host, port)]

    for a in answer:
        iplist.append(a['host'])

    # XXX check if the ip is a private one (10/8, 196.168/16, loopback, etc and don't go there)
    #  we should still log the IP to warc even if private
    # XXX log ip to warc here?
    return iplist

async def fetch(url, parts, session, config, headers=None, proxy=None, mock_url=None, allow_redirects=None, stats_me=True):

    maxsubtries = int(config['Crawl']['MaxSubTries'])
    pagetimeout = float(config['Crawl']['PageTimeout'])
    retrytimeout = float(config['Crawl']['RetryTimeout'])

    ret = namedtuple('fetcher_return', ['response', 'body_bytes', 'header_bytes',
                                        't_first_byte', 't_last_byte', 'last_exception'])

    if proxy: # pragma: no cover
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

        try:
            t0 = time.time()
            last_exception = None

            if len(iplist) == 0:
                iplist = await prefetch_dns(parts, mock_url, session)

            with stats.coroutine_state('fetcher fetching'):
                with aiohttp.Timeout(pagetimeout):
                    response = await session.get(mock_url or url,
                                                 allow_redirects=allow_redirects,
                                                 headers=headers)
                t_first_byte = '{:.3f}'.format(time.time() - t0)

                # XXX special sleepy 503 handling here - soft fail
                # XXX json_log tries
                # XXX serverdisconnected is a soft fail
                # XXX aiodns.error.DNSError
                # XXX equivalent to requests.exceptions.SSLerror ??
                #   reddit.com is an example of a CDN-related SSL fail
                # XXX when we retry, if local_addr was a list, switch to a different IP
                #   (change out the TCPConnector)
                # XXX what does a proxy error look like?
                # XXX record proxy error

                # fully receive headers and body.
                # XXX if we want to limit bytecount, do it here?
                body_bytes = await response.read()
                header_bytes = response.raw_headers
                t_last_byte = '{:.3f}'.format(time.time() - t0)

            # break only if we succeeded. 5xx = fail
            if response.status < 500:
                break
            print('retrying url={} code={}'.format(url, response.status))

#        ClientError
#            ClientConnectionError -- socket-related stuff
#                ClientOSError(ClientConnectionError, builtins.OSError) -- errno is set
#                ClientTimeoutError(ClientConnectionError, concurrent.futures._base.TimeoutError)
#                FingerprintMismatch -- SSL-related
#                ProxyConnectionError -- opening connection to proxy
#            ClientHttpProcessingError
#                ClientRequestError -- connection error during sending request
#                ClientResponseError -- connection error during reading response
#        DisconnectedError
#            ClientDisconnectedError
#                WSClientDisconnectedError -- deprecated
#            ServerDisconnectedError
#        HttpProcessingError
#            BadHttpMessage -- 400
#                BadStatusLine "200 OK"
#                HttpBadRequest -- 400
#                InvalidHeader
#                LineTooLong
#            HttpMethodNotAllowed -- 405
#            HttpProxyError -- anything other than success starting to talk to the proxy
#            WSServerHandshakeError -- websocket-related

            # actually seen:
            #  aiodns.error.DNSError - answer had no data
            #  asyncio.TimeoutError
            #  ClientResponseError - about.com robots {should be redir to www}, duowan.com robots {should be redir and then 404}
            #  ClientOSError - cntv.cn/robots.txt, errno=113 "No route to host"

        except (aiohttp.ClientError, aiohttp.DisconnectedError, aiohttp.HttpProcessingError,
                aiodns.error.DNSError, asyncio.TimeoutError) as e:
            last_exception = repr(e)
            LOGGER.debug('we sub-failed once, url is %s, exception is %s',
                         mock_url or url, last_exception)
        except Exception as e:
            last_exception = repr(e)
            print('UNKNOWN EXCEPTION SEEN in the fetcher')
            traceback.print_exc()
            LOGGER.debug('we sub-failed once WITH UNKNOWN EXCEPTION, url is %s, exception is %s',
                         mock_url or url, last_exception)

        if response:
            # if the exception was thrown during reading body_bytes, there will be a response object
            await response.release()

        # treat all 5xx somewhat similar to a 503: slow down and retry
        # XXX record 5xx so that everyone else slows down, too
        with stats.coroutine_state('fetcher retry sleep'):
            await asyncio.sleep(retrytimeout)

        subtries += 1
    else:
        if last_exception:
            LOGGER.debug('we failed, the last exception is %s', last_exception)
            return ret(None, None, None, None, None, last_exception)
        # fall through for the case of response.status >= 500

    if stats_me:
        stats.stats_sum('URLs fetched', 1)
        stats.stats_sum('fetch http code=' + str(response.status), 1)

    # checks after fetch:
    # hsts? if ssl, check strict-transport-security header,
    #   remember max-age=foo part., other stuff like includeSubDomains
    # did we receive cookies? was the security bit set?
    # record everything needed for warc. all headers, for example.

    return ret(response, body_bytes, header_bytes, t_first_byte, t_last_byte, None)

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
