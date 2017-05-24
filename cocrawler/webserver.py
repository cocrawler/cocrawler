import logging
from aiohttp import web

from . import config

LOGGER = logging.getLogger(__name__)


def make_app(loop):
    # TODO switch this to socket.getaddrinfo() -- see https://docs.python.org/3/library/socket.html
    serverip = config.read('REST', 'ServerIP')
    if serverip is None:
        return None
    serverport = int(config.read('REST', 'ServerPort'))

    app = web.Application()
    app.router.add_get('/', frontpage)
    app.router.add_get('/api/{name}', api)

    handler = app.make_handler()
    f = loop.create_server(handler, serverip, serverport)
    srv = loop.run_until_complete(f)
    LOGGER.info('REST serving on %s', srv.sockets[0].getsockname())

    app['cocrawler'] = handler, srv, loop
    return app


def close(app):
    if app is None:
        return

    handler, srv, loop = app['cocrawler']
    srv.close()
    loop.run_until_complete(srv.wait_closed())
    loop.run_until_complete(app.shutdown())
    loop.run_until_complete(handler.finish_connections(60.0))
    loop.run_until_complete(app.cleanup())


async def frontpage(request):
    return web.Response(text='Hello, world!')


async def api(request):
    name = request.match_info['name']
    data = {'name': name}
    return web.json_response(data)
