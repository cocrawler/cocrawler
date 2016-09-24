#!/usr/bin/env python
'''
A bottle-based webservice to work with burner. Does NOT use gevent
on purpose; it's intended to hog a single core.

It is important to enforce Host: localhost in the headers, elsewise
there's an odd way that people can exploit us.
'''

import argparse
import sys

from gevent import monkey
monkey.patch_all()
from bottle import route, run, BaseRequest, request, post, abort
BaseRequest.MEMFILE_MAX = 1024 * 1024 * 1024

import parse

ARGS = argparse.ArgumentParser(description='cocrawler burner helper')
ARGS.add_argument('--port', action='store', default=4321)
ARGS.add_argument('--cpu', action='store')

actions = {'find_html_links': parse.find_html_links,
           'find_html_links_and_embeds': parse.find_html_links_and_embeds,}

def do_action(host, name, data):
    if name not in actions:
        abort(404, 'unknown verb {}'.format(name))

    r1, r2 = actions[name](data.data)
    r1 = list(r1)
    r2 = list(r2)
    return {'verb': name, 'answer': {'links': r1, 'embeds': r2}}

# bottle stuff ------------------------------------------------------------

@post('/<name>/')
def action(name):
    host = request.get_header('Host')
    return do_action(host, name, request.forms)

args = ARGS.parse_args()
if args.cpu:
    print('cpu command-line arg not yet implemented', file=sys.stderr)

run(host='localhost', port=int(args.port))

print('AOOOOOOOOOOOGA PARSER-SERVER IS EXITING', file=sys.stderr)
