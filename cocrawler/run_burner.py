import os
import sys
import logging
import functools

import asyncio

import burner
import parse
import stats

test_threadcount = 2
loop = asyncio.get_event_loop()
b = burner.Burner(test_threadcount, loop, 'parser')
queue = asyncio.Queue()

def parse_all(name, string):
    links1, _ = parse.find_html_links(string, url=name)
    links2, embeds2 = parse.find_html_links_and_embeds(string, url=name)

    all2 = links2.union(embeds2)

    if len(links1) != len(all2):
        print('{} had different link counts of {} and {}'.format(name, len(links1), len(all2)))
        extra1 = links1.difference(all2)
        extra2 = all2.difference(links1)
        print('  extra in links:            {!r}'.format(extra1))
        print('  extra in links and embeds: {!r}'.format(extra2))
    return 1,

async def work():
    while True:
        w = await queue.get()
        with open(w, 'r', errors='ignore') as f:
            string = f.read()
        partial = functools.partial(parse_all, w, string)
        ret = await b.burn(partial)
        queue.task_done()

async def crawl():
    workers = [asyncio.Task(work(), loop=loop) for _ in range(test_threadcount)]
    print('q count is {}'.format(queue.qsize()))
    await queue.join()
    print('join is done')
    for w in workers:
        if not w.done():
            w.cancel()

# Main program:

for d in sys.argv[1:]:
    if os.path.isfile(d):
        queue.put_nowait(d)
        continue
    for root, _, files in os.walk(d):
        for name in files:
            if name.endswith('.html') or name.endswith('.htm'):
                queue.put_nowait(os.path.join(root, name))

print('Queue size is {}, beginning work.'.format(queue.qsize()))

try:
    loop.run_until_complete(crawl())
    print('exit run until complete')
except KeyboardInterrupt:
    sys.stderr.flush()
    print('\nInterrupt. Exiting cleanly.\n')
finally:
    loop.stop()
    loop.run_forever()
    loop.close()

levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
logging.basicConfig(level=levels[3])
stats.report()
