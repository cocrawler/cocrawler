'''
A single-queue global scheduler for CoCrawler

global QPS value used for all hosts

remember last deadline for every host

hand out work in order, increment deadlines

'''
import time
import asyncio
import uvloop
import pickle
from collections import defaultdict
from operator import itemgetter
import logging

import cachetools.ttl

from . import config
from . import stats

LOGGER = logging.getLogger(__name__)


class FixupEventLoopPolicy(uvloop.EventLoopPolicy):
    '''
    pytest-asyncio is weird and this is one way to work around that.
    https://github.com/pytest-dev/pytest-asyncio/issues/38
    '''
    def new_event_loop(self):
        if self._local._set_called:
            # raise RuntimeError('An event loop has already been set')
            loop = super().get_event_loop()
            if loop.is_closed():
                loop = super().new_event_loop()
            return loop
        return super().new_event_loop()


class Scheduler:
    '''
    Singleton to hold our globals. (Cue argument about singletons.)

    Kludge added for uvloop because this is the first thing to make a loop
    '''
    def __init__(self):
        asyncio.set_event_loop_policy(FixupEventLoopPolicy())
        self.q = asyncio.PriorityQueue()
        self.ridealong = {}
        self.awaiting_work = 0
        self.maxhostqps = None
        self.delta_t = None
        self.remaining_url_budget = None
        self.next_fetch = cachetools.ttl.TTLCache(10000, 10)  # 10 seconds good enough for QPS=0.1 and up
        self.frozen_until = cachetools.ttl.TTLCache(10000, 10)  # 10 seconds is longer than our typical delay


s = Scheduler()


def configure():
    s.maxhostqps = float(config.read('Crawl', 'MaxHostQPS'))
    s.delta_t = 1./s.maxhostqps
    s.remaining_url_budget = int(config.read('Crawl', 'MaxCrawledUrls') or 0) or None  # 0 => None


async def get_work():
    '''
    This function is called separately by each worker. It's allowed to sleep and/or requeue
    if work can't be done immediately.
    '''
    while True:
        try:
            work = s.q.get_nowait()
        except asyncio.queues.QueueEmpty:
            # using awaiting_work to see if all workers are idle can race with sleeping s.q.get()
            # putting it in an except clause makes sure the race is only run when
            # the queue is actually empty.
            s.awaiting_work += 1
            with stats.coroutine_state('awaiting work'):
                work = await s.q.get()
            s.awaiting_work -= 1

        if s.remaining_url_budget is not None and s.remaining_url_budget <= 0:
            s.q.put_nowait(work)
            s.q.task_done()
            raise asyncio.CancelledError

        now = time.time()
        surt = work[2]
        surt_host, _, _ = surt.partition(')')
        ridealong = get_ridealong(surt)

        recycle, why, dt = do_we_recycle(now, surt, surt_host, ridealong)

        # sleep then requeue
        if recycle:
            stats.stats_sum(why, dt)
            with stats.coroutine_state(why):
                await asyncio.sleep(dt)
                s.q.put_nowait(work)
                s.q.task_done()
                continue

        # Normal case: sleep if needed, and then return the work to the caller.
        s.next_fetch[surt_host] = now + dt + s.delta_t
        if dt > 0:
            stats.stats_sum(why, dt)
            with stats.coroutine_state(why):
                await asyncio.sleep(dt)

        if s.remaining_url_budget is not None:
            s.remaining_url_budget -= 1
        return work


def do_we_recycle(now, surt, surt_host, ridealong):
    recycle = False
    why = None
    dt = 0

    # does host have cached dns? XXX
    # if not, and we're The One, fetch it
    # if not, and we aren't The One, recycle

    # does host have cached robots? XXX
    # if not, and we're The One, fetch it
    # if not, and we aren't The One, recycle

    # when's the next available rate limit slot?
    now = time.time()
    if surt_host in s.next_fetch:
        dt = max(s.next_fetch[surt_host] - now, 0.)
    else:
        dt = 0
    if dt > 3.0:
        recycle = True
        why = 'scheduler ratelimit recycle'
        dt = 3.0
    elif dt > 0:
        why = 'scheduler ratelimit short sleep'

    return recycle, why, dt


def work_done():
    s.q.task_done()


def requeue_work(work):
    '''
    When we requeue work after a failure, we add 0.5 to the rand;
    eventually do that in here
    '''
    s.q.put_nowait(work)


def queue_work(work):
    s.q.put_nowait(work)


def qsize():
    return s.q.qsize()


def set_ridealong(ridealongid, work):
    s.ridealong[ridealongid] = work


def get_ridealong(ridealongid):
    if ridealongid in s.ridealong:
        return s.ridealong[ridealongid]
    else:
        LOGGER.warning('ridealong data for surt %s not found', ridealongid)
        return {}


def del_ridealong(ridealongid):
    del s.ridealong[ridealongid]


def done(worker_count):
    return s.awaiting_work == worker_count and s.q.qsize() == 0


async def close():
    await s.q.join()


def save(crawler, f):
    # XXX make this more self-describing
    pickle.dump('Put the XXX header here', f)  # XXX date, conf file name, conf file checksum
    pickle.dump(s.ridealong, f)
    pickle.dump(crawler._seeds, f)
    count = s.q.qsize()
    pickle.dump(count, f)
    for _ in range(0, count):
        work = s.q.get_nowait()
        pickle.dump(work, f)


def load(crawler, f):
    header = pickle.load(f)  # XXX check that this is a good header... log it
    s.ridealong = pickle.load(f)
    crawler._seeds = pickle.load(f)
    s.q = asyncio.PriorityQueue()
    count = pickle.load(f)
    for _ in range(0, count):
        work = pickle.load(f)
        s.q.put_nowait(work)


def summarize():
    '''
    Print a human-readable summary of what's in the queues
    '''
    print('{} items in the crawl queue'.format(s.q.qsize()))
    print('{} items in the ridealong dict'.format(len(s.ridealong)))

    if s.q.qsize() != len(s.ridealong):
        LOGGER.error('Different counts for queue size and ridealong size')
        q_keys = set()
        try:
            while True:
                priority, rand, surt = s.q.get_nowait()
                q_keys.add(surt)
        except asyncio.queues.QueueEmpty:
            pass
        ridealong_keys = set(s.ridealong.keys())
        extra_q = q_keys.difference(ridealong_keys)
        extra_r = ridealong_keys.difference(q_keys)
        if extra_q:
            print('Extra urls in queues and not ridealong')
            print(extra_q)
        if extra_r:
            print('Extra urls in ridealong and not queues')
            print(extra_r)
            for r in extra_r:
                print('  ', r, s.ridealong[r])
        raise ValueError('cannot continue, I just destroyed the queue')

    urls_with_tries = 0
    priority_count = defaultdict(int)
    netlocs = defaultdict(int)
    for k, v in s.ridealong.items():
        if 'tries' in v:
            urls_with_tries += 1
        priority_count[v['priority']] += 1
        url = v['url']
        netlocs[url.urlsplit.netloc] += 1

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


def update_priority(priority, rand):
    '''
    When a fail occurs, we get requeued with a bigger 'rand'.
    This means that as we crawl in a given priority, we accumulate
    more and more repeatedly failing pages as we get close to the
    end of the queue. This function increments the priority if
    rand is too large, kicking the can down the road.
    '''
    while rand > 1.2:
        priority += 1
        rand -= 1.0
    return priority, rand
