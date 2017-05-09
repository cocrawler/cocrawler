'''
Fetches some urls using aiohttp. Also serves as a minimum example of using aiohttp.

Good examples:

https://www.enterprisecarshare.com/robots.txt -- 302 redir lacking Location: raises RuntimeError

'''

import sys
from traceback import print_exc

import asyncio
import aiohttp


async def main(urls):
    for url in urls:
        if not url.startswith('http'):
            url = 'http://' + url
        async with aiohttp.ClientSession() as session:
            print(url, '\n')
            try:
                response = await session.get(url, allow_redirects=True)
            except Exception as e:
                print_exc()
                print('')
                continue

            if hasattr(session, 'last_req'):
                print('final request headers:')
                for k, v in session.last_req.headers.items():
                    print(k+':', v)
                print('')

            for h in response.history:
                print(h)
            print('')
            for h in response.raw_headers:
                line = h[0]+b': '+h[1]
                print(line.decode(errors='ignore'))
            print('')

            try:
                print(await response.text(errors='ignore'))
            except Exception as e:
                print_exc()

loop = asyncio.get_event_loop()

loop.run_until_complete(main(sys.argv[1:]))
