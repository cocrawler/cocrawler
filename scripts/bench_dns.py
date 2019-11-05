'''
Benchmark the configured DNS service.

Verify that DNS more-or-less works. Measure its speed to see if it'll
be a bottleneck.
'''

import sys
import time
import argparse
import asyncio
import random
import os

from cocrawler.urls import URL
import cocrawler.dns as dns
import cocrawler.config as config

ARGS = argparse.ArgumentParser(description='CoCrawler dns benchmark')
ARGS.add_argument('--config', action='append')
ARGS.add_argument('--configfile', action='store')
ARGS.add_argument('--count', type=int, default=1000)
ARGS.add_argument('--expect-not-suitable', action='store_true')

args = ARGS.parse_args()

config.config(args.configfile, args.config)
max_workers = config.read('Crawl', 'MaxWorkers')
ns = config.read('Fetcher', 'Nameservers')
if isinstance(ns, str):
    ns = [ns]
    config.write(ns, 'Fetcher', 'Nameservers')

exit_value = 0

resolver = dns.get_resolver()


def create_queue():
    queue = asyncio.Queue()

    # add a fake domain to make sure the dns doesn't send unknown hosts to a search
    # note that mail.foo.com and mx.foo.com don't generally get bogus answers, it's foo.com or www.foo.com that do
    for _ in range(2):
        r = random.Random()
        host = str(r.randrange(1000000000)) + str(r.randrange(1000000000)) + str(r.randrange(1000000000))
        queue.put_nowait((URL('http://' + host + '.com'), 'fake'))

    # read list of domains to query -- from alexa top million
    head, tail = os.path.split(__file__)
    alexa = os.path.join(head, os.pardir, 'data', 'top-1k.txt')
    alexa_count = 0

    try:
        with open(alexa, 'r') as f:
            print('Using top-1k from Alexa, expect a few failures')
            for line in f:
                queue.put_nowait((URL('http://'+line.strip()), 'real'))
                alexa_count += 1
                if alexa_count > args.count:
                    break
    except FileNotFoundError:
        # the alexa file wasn't available (it is not in the repo) so just do a few
        print('Cannot find top-1k file, so all queries are www.google.com')
        for _ in range(args.count):
            queue.put_nowait((URL('http://www.google.com'), 'real'))
    return queue


async def work():
    while True:
        sys.stdout.flush()
        try:
            url, kind = queue.get_nowait()
        except asyncio.queues.QueueEmpty:
            break

        try:
            sys.stdout.flush()
            result = await dns.prefetch(url, resolver)
        except Exception as e:
            result = None
            if kind != 'fake':
                print('saw exception', e, 'but ignoring it')

        if result is not None and kind == 'fake':
            if args.expect_not_suitable:
                print('as expected, this nameserver is not suitable for crawling')
            else:
                print('invalid hostname got a result: your nameserver is not suitable for crawling')
                global exit_value
                exit_value = 1

        queue.task_done()


async def main():
    workers = [asyncio.Task(work()) for _ in range(max_workers)]

    await queue.join()

    for w in workers:
        if not w.done():
            w.cancel()

queue = create_queue()
qsize = queue.qsize()

print('workers:', max_workers)
print('configured nameservers:', ns)
print('queries in queue', qsize)

t0 = time.time()

loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(main())
except KeyboardInterrupt:
    print('interrupt seen, counts will not be accurate')
    sys.stdout.flush()
    sys.stderr.flush()
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
