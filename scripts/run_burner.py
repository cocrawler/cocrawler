import os
import sys
import logging
import functools

import asyncio

import cocrawler.burner as burner
import cocrawler.parse as parse
import cocrawler.stats as stats
import cocrawler.config as config


c = {'Multiprocess': {'BurnerThreads': 2}}
config.set_config(c)
loop = asyncio.get_event_loop()
b = burner.Burner('parser')
queue = asyncio.Queue()


def parse_all(name, string):
    links1, _ = parse.find_html_links_re(string)
    links2, embeds2 = parse.find_html_links_re(string)  # XXX

    links1 = set(parse.collapse_links(links1))
    links2 = set(parse.collapse_links(links2))
    embeds2 = set(parse.collapse_links(embeds2))

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
        with open(w, 'r', errors='ignore') as fi:
            string = fi.read()
        partial = functools.partial(parse_all, w, string)
        await b.burn(partial)
        queue.task_done()


async def crawl():
    workers = [asyncio.Task(work(), loop=loop) for _ in range(int(config.read('Multiprocess', 'BurnerThreads')))]
    print('q count is {}'.format(queue.qsize()))
    await queue.join()
    print('join is done')
    for w in workers:
        if not w.done():
            w.cancel()


def main():
    for d in sys.argv[1:]:
        if os.path.isfile(d):
            queue.put_nowait(d)
            continue
        for root, _, files in os.walk(d):
            for f in files:
                if f.endswith('.html') or f.endswith('.htm'):
                    queue.put_nowait(os.path.join(root, f))

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


if __name__ == '__main__':
    # this guard needed for MacOS and Windows
    main()
