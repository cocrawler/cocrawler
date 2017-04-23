'''
Benchmark the configured DNS service.

Verify that DNS more-or-less works. Measure its speed to see if it'll
be a bottleneck.
'''

import sys
import time
import argparse
import asyncio
import socket
import random
import os

import aiohttp

import cocrawler.conf as conf
import cocrawler.dns as dns

ARGS = argparse.ArgumentParser(description='CoCrawler dns benchmark')
ARGS.add_argument('--config', action='append')
ARGS.add_argument('--configfile', action='store')
ARGS.add_argument('--no-confighome', action='store_true')
ARGS.add_argument('--count', type=int, default=1000)

args = ARGS.parse_args()

config = conf.config(args.configfile, args.config, confighome=not args.no_confighome)
max_workers = config['Crawl']['MaxWorkers']

ns = config['Fetcher'].get('Nameservers')
if not isinstance(ns, list):
    ns = [ns]

#resolver = aiohttp.resolver.AsyncResolver(nameservers=ns)  # Can I pass rotate=True into this?
#connector = aiohttp.connector.TCPConnector(resolver=resolver, family=socket.AF_INET)
#session = aiohttp.ClientSession(connector=connector)
exit_value = 0

dns.setup_resolver(ns)


def create_queue():
    queue = asyncio.Queue()

    # add a fake domain to make sure the dns doesn't send unknown hosts to a search
    # note that mail.foo.com and mx.foo.com don't generally get bogus answers, it's foo.com or www.foo.com that do
    for _ in range(2):
        r = random.Random()
        host = str(r.randrange(1000000000)) + str(r.randrange(1000000000)) + str(r.randrange(1000000000))
        queue.put_nowait(host + '.com')

    # read list of domains to query -- from alexa top million
    head, tail = os.path.split(__file__)
    alexa = os.path.join(head, os.pardir, 'examples', 'top-1k.txt')
    alexa_count = 0
    with open(alexa, 'r') as f:
        for line in f:
            queue.put_nowait(line.strip())
            alexa_count += 1
            if alexa_count > args.count:
                break

    return queue

async def work():
    while True:
        try:
            host = queue.get_nowait()
        except asyncio.queues.QueueEmpty:
            break

        try:
            result = await dns.query(host, 'A')
        except Exception as e:
            result = None
            print('saw exception', e, 'but ignoring it')

        if len(host) > 29 and result is not None:
            print('invalid hostname got a result: your nameserver is not suitable for crawling')
            global exit_value
            exit_value = 1

        queue.task_done()


async def main():
    workers = [asyncio.Task(work(), loop=loop) for _ in range(max_workers)]
    await queue.join()
    for w in workers:
        if not w.done():
            w.cancel()

queue = create_queue()
qsize = queue.qsize()

print('workers:', max_workers)
print('configured nameservers:', ns)

t0 = time.time()

loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(main())
except KeyboardInterrupt:
    sys.stderr.flush()
    sys.stdout.flush()
finally:
    loop.stop()
    loop.run_forever()
    loop.close()
    #session.close()

elapsed = time.time() - t0
if not elapsed:
    elapsed = 1
qps = qsize / elapsed

print('processed {} dns calls in {:.1f} seconds, qps = {:.1f}'.format(qsize, elapsed, qps))

sys.exit(exit_value)
