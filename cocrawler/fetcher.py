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
import aiohttp

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

async def fetch(url, headers=None, proxy=None, mock_url=None):
    if proxy:
        proxy = aiohttp.ProxyConnector(proxy=proxy)
        # we need to preserve the existing connector config (see cocrawler.__init__)
        # XXX need to research how to do this
        raise ValueError('not yet implemented')





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


