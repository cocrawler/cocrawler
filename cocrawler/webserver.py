import logging
import asyncio
from aiohttp import web

from . import config

LOGGER = logging.getLogger(__name__)


def make_app():
    loop = asyncio.get_event_loop()
    # TODO switch this to socket.getaddrinfo() -- see https://docs.python.org/3/library/socket.html
    serverip = config.read('REST', 'ServerIP')
    if serverip is None:
        return None
    serverport = config.read('REST', 'ServerPort')

    increment = False
    if isinstance(serverport, str) and serverport.endswith('+'):
        increment = True
        serverport = serverport[:-1]

    app = web.Application()
    app.router.add_get('/', frontpage)
    app.router.add_get('/api/{name}', api)

    # aiohttp 3.0 has AppRunner(). maybe I should switch to it?
    # also web.run_app(app, access_log=None) to turn off logging

    handler = app.make_handler()

    while True:
        try:
            f = loop.create_server(handler, serverip, serverport)
            break
        except OSError as e:  # address already in use
            if increment:
                LOGGER.info('OSError starting webserver: %s', repr(e))
                serverport += 1
                LOGGER.info('incrementing port to %d', serverport)
            else:
                raise

    srv = loop.run_until_complete(f)
    LOGGER.info('REST serving on %s', srv.sockets[0].getsockname())

    app['cocrawler'] = handler, srv
    return app


def close(app):
    if app is None:
        return

    handler, srv = app['cocrawler']
    loop = asyncio.get_event_loop()
    srv.close()
    loop.run_until_complete(srv.wait_closed())
    loop.run_until_complete(app.shutdown())
    loop.run_until_complete(app.cleanup())


async def frontpage(request):
    return web.Response(text='Hello, world!')


async def api(request):
    name = request.match_info['name']
    data = {'name': name}
    return web.json_response(data)
