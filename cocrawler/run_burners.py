'''
Runs all of the available parsers over a tree of html

Accumulate cpu time
Compare counts of urls and embeds
'''

import asyncio

import sys
import os
import logging
import json
import time

import stats
import parse
import burners

async def parse_all(name, string, b):

    print('length of string is {}'.format(len(string)))

    t0 = time.time()
    ret1 = await b.post('find_html_links', string)
    elapsed = time.time() - t0
    if elapsed > 0:
        print('find_html_links is {:.3f} MB/s elapsed'.format(len(string) / 1024. / 1024. / elapsed))
    t0 = time.time()
    ret2 = await b.post('find_html_links_and_embeds', string)
    elapsed = time.time() - t0
    if elapsed > 0:
        print('find_html_links_and_embeds is {:.3f} MB/s elapsed'.format(len(string) / 1024. / 1024. / elapsed))

    links1 = set(ret1.get('links', []))
    links2, embeds2 = set(ret2.get('links', [])), set(ret2.get('embeds', []))

    all2 = links2.union(embeds2)

    if len(links1) != len(all2):
        print('{} had different link counts of {} and {}'.format(name, len(links1), len(all2)))
        extra1 = links1.difference(all2)
        extra2 = all2.difference(links1)
        print('  extra in links:            {!r}'.format(extra1))
        print('  extra in links and embeds: {!r}'.format(extra2))
    return

async def work():
    try:
        while True:
            expanded = await q.get()
            print('working on {}'.format(expanded))
            with open(expanded, 'r', errors='ignore') as f:
                await parse_all(expanded, f.read(), b)
            q.task_done()
    except asyncio.CancelledError:
        pass

async def all_work():
    print('starting all_work, queue is {} files'.format(q.qsize()))
    workers = [asyncio.Task(work(), loop=loop) for _ in range(2)]
    await q.join()
    print('join finished, exiting')
    print('ending all_work, queue is {} files'.format(q.qsize()))
    for w in workers:
        w.cancel()

config = {}
b = burners.Burner('./parser-server.py', 2, config)
loop = asyncio.get_event_loop()
q = asyncio.Queue(loop=loop)

for d in sys.argv[1:]:
    for root, _, files in os.walk(d):
        for name in files:
            if name.endswith('.html') or name.endswith('.htm'):
                expanded = os.path.join(root, name)
                q.put_nowait(expanded)
    else:
        if os.path.isfile(d):
            q.put_nowait(d)
            
loop.run_until_complete(all_work())

levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
logging.basicConfig(level=levels[3])
stats.report()
