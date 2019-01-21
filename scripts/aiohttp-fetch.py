'''
Fetches some urls using aiohttp. Also serves as a minimum example of using aiohttp.

Good examples:

https://www.enterprisecarshare.com/robots.txt -- 302 redir lacking Location: raises RuntimeError

'''

import sys
from traceback import print_exc

import asyncio
import aiohttp
import aiohttp.connector


async def main(urls):
    connector = aiohttp.connector.TCPConnector(use_dns_cache=True)
    session = aiohttp.ClientSession(connector=connector)

    for url in urls:
        if not url.startswith('http'):
            url = 'http://' + url

        print(url, '\n')
        try:
            response = await session.get(url, allow_redirects=True)
        except aiohttp.client_exceptions.ClientConnectorError as e:
            print('saw connect error for', url, ':', e, file=sys.stderr)
            continue
        except Exception as e:
            print('Saw an exception thrown by session.get:')
            print_exc()
            print('')
            continue

        #print('dns:')
        #for k, v in connector.cached_hosts.items():
        #    print('  ', k)  # or k[0]?
        #    for rec in v:
        #        print('    ', rec.get('host'))

        print('')
        if str(response.url) != url:
            print('final url:', str(response.url))
            print('')

        print('final request headers:')
        for k, v in response.request_info.headers.items():
            print(k+':', v)
        print('')

        if response.history:
            print('response history: response and headers:')
            for h in response.history:
                print('  ', repr(h))
            print('')

            print('response history urls:')
            response_urls = [str(h.url) for h in response.history]
            response_urls.append(str(response.url))
            if response_urls:
                print('  ', '\n   '.join(response_urls))
                print('')

        print('response headers:')
        for k, v in response.raw_headers:
            line = k+b': '+v
            print('  ', line.decode(errors='ignore'))
        print('')

        try:
            text = await response.text(errors='ignore')
            #print(text)
            pass
        except Exception:
            print_exc()

    await session.close()

loop = asyncio.get_event_loop()

loop.run_until_complete(main(sys.argv[1:]))

# vodoo recommended by advanced aiohttp docs for graceful shutdown
# https://github.com/aio-libs/aiohttp/issues/1925
loop.run_until_complete(asyncio.sleep(0.250))
loop.close()
