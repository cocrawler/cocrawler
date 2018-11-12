#!/usr/bin/env python
'''
A bottle+gevent based webservice suitable for testing CoCrawler
'''

try:
    from gevent import monkey
    monkey.patch_all()
except ImportError:
    print('gevent not present; that\'s OK for test purposes')
    pass

import os
import random
from bottle import hook, route, run, request, abort, redirect
from urllib.parse import urlsplit


def generate_robots(host):
    if host.startswith('robotsdenyall'):
        return 'User-Agent: *\nDisallow: /\n'
    if host.startswith('404'):
        abort(404, 'No robots.txt here')
    if host.startswith('500'):
        abort(500, 'I don\'t know what I\'m doing!')
    if host.startswith('302loop'):
        redirect('http://127.0.0.1:8080/robots.txt.302loop')  # infinite loop
    if host.startswith('302'):
        # unfortunately, we can't use a fake hostname here.
        # XXX figure out how to get this constant out of here... header?
        redirect('http://127.0.0.1:8080/robots.txt.302')
    if host.startswith('pdfrobots'):
        return '%PDF-1.3\n'
    return 'User-Agent: *\nDisallow: /denied/\n'


def generate_robots_302(host):
    host = 'do-not-redirect-me'
    return generate_robots(host)


def generate_robots_302loop(host):
    return generate_robots(host)


siteheader = '''<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
'''
sitelink = '<url><loc>/ordinary/{}</loc></url>\n'
sitefooter = '</urlset>\n'


def generate_sitemap(host):
    mylinks = ''
    for i in range(10):
        mylinks += sitelink.format(i)
    return siteheader + mylinks + sitefooter


header = '''
<html><head><title>Title</title></head><body>
'''

links = '''<ul>
<li><a href="{}">next link</a>
<li><a href="{}">next link</a>
<li><a href="/denied/">but not this one</a>
</ul>
'''

trailer = '''
</body></html>
'''


def generate_ordinary(name, host):
    # send 302.foo/ordinary/0 to 302.foo/ordinary/1, which will not be a redirect
    if host.startswith('302') and name <= 0:
        redirect('/ordinary/{}'.format(name+1))
    if host.startswith('503'):
        abort(503, 'Slow down, you move too fast. You got to make the morning last.\n')

    mylinks = links.format((name+1) % 1000, (2*name) % 1000)
    return header + mylinks + trailer


def generate_ordinary_503s(name, host):
    if random.randint(1, 9) < 2:  # 10% chance
        abort(503, 'Slow down, you move too fast. You got to make the morning last.\n')
    return generate_ordinary(name, host)


def generate_code(code, host):
    abort(code, 'Here is your code {}; host is {}\n'.format(code, host))


def generate_trap(name, host):
    mylinks = links.format(name+1, 2*name)
    return header + mylinks + trailer

# bottle stuff ------------------------------------------------------------


@hook('before_request')
def strip_proxy_host():
    # the test system uses this webserver as a forward proxy.
    # strip the host from PATH if present, it will still be in the Host: header
    if 'PATH_INFO' in request.environ:
        full = request.environ['PATH_INFO']
        if full.startswith('http://') or full.startswith('https://'):
            parts = urlsplit(full)
            url = parts.path
            if parts.query:
                url += '?' + parts.query
            request.environ['PATH_INFO'] = url


@route('/hello')
def hello():
    return 'Hello World! Host is {}\n'.format(request.get_header('Host'))


@route('/robots.txt')
def robots():
    host = request.get_header('Host')
    return generate_robots(host)


@route('/robots.txt.302')
def robots302():
    host = request.get_header('Host')
    return generate_robots_302(host)


@route('/robots.txt.302loop')
def robots302():
    host = request.get_header('Host')
    return generate_robots_302loop(host)


@route('/sitemap.xml')
def sitemap():
    host = request.get_header('Host')
    return generate_sitemap(host)


@route('/ordinary/<name:int>')
def ordinary(name):
    host = request.get_header('Host')
    return generate_ordinary(name, host)


@route('/ordinary-with-503s/<name:int>')
def ordinary503(name):
    host = request.get_header('Host')
    return generate_ordinary_503s(name, host, ua)


@route('/code/<code:int>')
def code(code):
    host = request.get_header('Host')
    return generate_code(code, host)


@route('/trap/<name:int>/')
def trap(name):
    host = request.get_header('Host')
    return generate_trap(name, host)


port = os.getenv('PORT') or 8080
run(host='localhost', port=port)
