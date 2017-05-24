'''
A single-queue global scheduler for CoCrawler

global QPS value used for all hosts

remember last deadline for every host

hand out work in order, increment deadlines

'''
import asyncio
import pickle
from collections import defaultdict

from . import stats

q = asyncio.PriorityQueue()
ridealong = {}
awaiting_work = 0

# XXX Crawler.ridealongmaxid is still in cocrawler/__init__

# I don't think this is necessary
# def init_queue(loop):
#    global q
#    q = asyncio.PriorityQueue(loop=loop)


async def get_work():
    try:
        work = q.get_nowait()
    except asyncio.queues.QueueEmpty:
        # using awaiting_work to see if we're idle can race with sleeping q.get()
        # putting it in an except clause makes sure the race is only run when
        # the queue is actually empty.
        global awaiting_work
        awaiting_work += 1
        with stats.coroutine_state('awaiting work'):
            work = await q.get()
        awaiting_work -= 1
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


def len_ridealong():
    return len(ridealong)


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
    urls_with_tries = 0
    priority_count = defaultdict(int)
    netlocs = defaultdict(int)
    for k, v in ridealong.items():
        if 'tries' in v:
            urls_with_tries += 1
        priority_count[v['priority']] += 1
        url = v['url']
        netlocs[url.urlparse.netloc] += 1

    return urls_with_tries, netlocs, priority_count
