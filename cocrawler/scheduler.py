'''
A single-queue global scheduler for CoCrawler

global QPS value used for all hosts

remember last deadline for every host

hand out work in order, increment deadlines

'''
import time
import asyncio
import pickle
from collections import defaultdict
from operator import itemgetter
import logging

import cachetools.ttl

from . import config
from . import stats

LOGGER = logging.getLogger(__name__)

q = asyncio.PriorityQueue()
ridealong = {}
awaiting_work = 0
global_qps = None
global_delta_t = None
remaining_url_budget = None
next_fetch = cachetools.ttl.TTLCache(10000, 10)  # 10 seconds good enough for QPS=0.1 and up


def configure():
    global global_qps
    global_qps = float(config.read('Crawl', 'MaxHostQPS'))
    global global_delta_t
    global_delta_t = 1./global_qps
    global remaining_url_budget
    remaining_url_budget = int(config.read('Crawl', 'MaxCrawledUrls') or 0) or None  # 0 => None


async def get_work():
    while True:
        global remaining_url_budget
        if remaining_url_budget is not None and remaining_url_budget <= 0:
            raise asyncio.CancelledError

        try:
            work = q.get_nowait()
        except asyncio.queues.QueueEmpty:
            # using awaiting_work to see if all workers are idle can race with sleeping q.get()
            # putting it in an except clause makes sure the race is only run when
            # the queue is actually empty.
            global awaiting_work
            awaiting_work += 1
            with stats.coroutine_state('awaiting work'):
                work = await q.get()
            awaiting_work -= 1

        # when can this work be done?
        surt = work[2]
        surt_host, _, _ = surt.partition(')')
        now = time.time()
        if surt_host in next_fetch:
            dt = max(next_fetch[surt_host] - now, 0.)
        else:
            dt = 0

        # If it's more than 3 seconds in the future, we are HOL blocked
        # requeue, sleep, repeat
        if dt > 3.0:
            q.put_nowait(work)
            q.task_done()
            stats.stats_sum('scheduler HOL sleep', dt)
            with stats.coroutine_state('scheduler HOL sleep'):
                await asyncio.sleep(3.0)
                continue

        # sleep and then do the work
        next_fetch[surt_host] = now + dt + global_delta_t
        if dt > 0:
            stats.stats_sum('scheduler short sleep', dt)
            with stats.coroutine_state('scheduler short sleep'):
                await asyncio.sleep(dt)

        if remaining_url_budget is not None:
            remaining_url_budget -= 1
        return work


def work_done():
    q.task_done()


def requeue_work(work):
    '''
    When we requeue work after a failure, we add 0.5 to the rand;
    eventually do that in here
    '''
    q.put_nowait(work)


def queue_work(work):
    q.put_nowait(work)


def qsize():
    return q.qsize()


def set_ridealong(ridealongid, work):
    ridealong[ridealongid] = work


def get_ridealong(ridealongid):
    return ridealong[ridealongid]


def del_ridealong(ridealongid):
    del ridealong[ridealongid]


def done(worker_count):
    return awaiting_work == worker_count and q.qsize() == 0


async def close():
    await q.join()


def save(crawler, f):
    # XXX make this more self-describing
    # XXX push down into scheduler.py
    pickle.dump('Put the XXX header here', f)  # XXX date, conf file name, conf file checksum
    pickle.dump(crawler.ridealongmaxid, f)
    pickle.dump(ridealong, f)
    pickle.dump(crawler._seeds, f)
    count = q.qsize()
    pickle.dump(count, f)
    for _ in range(0, count):
        work = q.get_nowait()
        pickle.dump(work, f)


def load(crawler, f):
    header = pickle.load(f)  # XXX check that this is a good header... log it
    crawler.ridealongmaxid = pickle.load(f)
    global ridealong
    ridealong = pickle.load(f)
    crawler._seeds = pickle.load(f)
    global q
    q = asyncio.PriorityQueue()
    count = pickle.load(f)
    for _ in range(0, count):
        work = pickle.load(f)
        q.put_nowait(work)


def summarize():
    '''
    Print a human-readable summary of what's in the queues
    '''
    print('{} items in the crawl queue'.format(q.qsize()))
    print('{} items in the ridealong dict'.format(len(ridealong)))

    urls_with_tries = 0
    priority_count = defaultdict(int)
    netlocs = defaultdict(int)
    for k, v in ridealong.items():
        if 'tries' in v:
            urls_with_tries += 1
        priority_count[v['priority']] += 1
        url = v['url']
        netlocs[url.urlparse.netloc] += 1

    print('{} items in crawl queue are retries'.format(urls_with_tries))
    print('{} different hosts in the queue'.format(len(netlocs)))
    print('Queue counts by priority:')
    for p in sorted(list(priority_count.keys())):
        if priority_count[p] > 0:
            print('  {}: {}'.format(p, priority_count[p]))
    print('Queue counts for top 10 netlocs')
    netloc_order = sorted(netlocs.items(), key=itemgetter(1), reverse=True)[0:10]
    for k, v in netloc_order:
        print('  {}: {}'.format(k, v))
