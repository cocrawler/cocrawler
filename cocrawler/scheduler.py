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
import json

from . import config
from . import stats
from . import memory
from . import dns
from . import fetcher

LOGGER = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, robots, resolver):
        self.robots = robots
        self.resolver = resolver

        self.q = asyncio.PriorityQueue()
        self.ridealong = {}
        self.awaiting_work = 0
        self.maxhostqps = None
        self.delta_t = None
        self.next_fetch = cachetools.ttl.TTLCache(10000, 10)  # 10 seconds good enough for QPS=0.1 and up
        self.frozen_until = cachetools.ttl.TTLCache(10000, 10)  # 10 seconds is longer than our typical delay
        self.maxhostqps = float(config.read('Crawl', 'MaxHostQPS'))
        self.delta_t = 1./self.maxhostqps
        self.initialize_budgets()

        _, prefetch_dns = fetcher.global_policies()
        self.use_ip_key = prefetch_dns
        memory.register_debug(self.memory)

    def initialize_budgets(self):
        self.budgets = {}
        self.budget_default = {}
        for which, conf in (('global_budget', 'GlobalBudget'), ('domain_budget', 'DomainBudget'),
                            ('host_budget', 'HostBudget')):
            value = config.read('Crawl', conf)
            self.budgets[which] = {}
            if value is not None:
                self.budget_default[which] = int(value)

    def check_budget(self, which, key):
        budget = self.budgets[which]
        if key not in budget and which in self.budget_default:
            budget[key] = self.budget_default[which]
        if key in budget:
            if budget[key] > 0:
                budget[key] -= 1
                return True
            else:
                return False
        else:
            return None

    def check_budgets(self, url):
        for budget, arg in (('host_budget', url.hostname_without_www),
                            ('domain_budget', url.registered_domain),
                            ('global_budget', None)):
            b = self.check_budget(budget, arg)
            if b is not None:
                return b
        return True

    def max_crawled_urls_exceeded(self):
        #if ((self.max_crawled_urls is not None and
        #     (stats.stat_value('fetch http code=200') or 0) >= self.max_crawled_urls)):
        #    return True
        return False

    async def get_work(self):
        '''
        This function is called separately by each worker. It's allowed to sleep and/or requeue
        if work can't be done immediately.
        '''
        while True:
            if self.max_crawled_urls_exceeded():
                raise asyncio.CancelledError  # cancel this one worker

            try:
                work = self.q.get_nowait()
            except asyncio.queues.QueueEmpty:
                # using awaiting_work to see if all workers are idle can race with sleeping s.q.get()
                # putting it in an except clause makes sure the race is only run when
                # the queue is actually empty.
                self.awaiting_work += 1
                with stats.coroutine_state('awaiting work'):
                    work = await self.q.get()
                self.awaiting_work -= 1

            if self.max_crawled_urls_exceeded():
                self.q.put_nowait(work)
                self.q.task_done()
                raise asyncio.CancelledError  # cancel this one worker

            surt = work[2]
            surt_host, _, _ = surt.partition(')')
            ridealong = self.get_ridealong(surt)

            recycle, why, dt = await self.schedule_work(surt, surt_host, ridealong)

            if recycle:
                # sleep then requeue
                stats.stats_sum(why+' sum', dt)
                with stats.coroutine_state(why):
                    await asyncio.sleep(dt)
                    self.q.put_nowait(work)
                    self.q.task_done()
                    continue

            # Normal case: sleep if needed, and then return the work to the caller.
            if dt > 0:
                stats.stats_sum(why+' sum', dt)
                with stats.coroutine_state(why):
                    await asyncio.sleep(dt)

            return work

    def next_slot(self, now, keys):
        times = [0.]
        for key in keys:
            if key in self.next_fetch:
                times.append(self.next_fetch[key] - now)
        return max(times)

    async def schedule_work(self, surt, surt_host, ridealong):
        recycle, why, dt = False, None, 0

        if self.robots.check_cached(ridealong['url'], quiet=True) == 'denied':
            # immediately fetch, it will fail robots and be logged
            recycle = False
            why = 'scheduler cached robots deny'
            return recycle, why, 0.

        if self.use_ip_key:
            dns_entry = await dns.prefetch(ridealong['url'], self.resolver)
            ip_key = dns.entry_to_ip_key(dns_entry)
        else:
            ip_key = None

        now = time.time()  # get time after potentially waiting for dns

        keys = [k for k in (ip_key, surt_host) if k]
        LOGGER.debug('keys are %r', keys)
        dt = self.next_slot(now, keys)
        LOGGER.debug('dt is %f', dt)

        if dt > 3.0:
            recycle = True
            why = 'scheduler ratelimit recycle'
            dt = 3.0
        elif dt > 0:
            why = 'scheduler ratelimit short sleep'
            for k in keys:
                self.next_fetch[k] = now + dt + self.delta_t
        else:
            for k in keys:
                self.next_fetch[k] = now + self.delta_t

        return recycle, why, dt

    def work_done(self):
        self.q.task_done()

    def requeue_work(self, work):
        '''
        When we requeue work after a failure, we add 0.5 to the rand;
        eventually do that in here
        '''
        self.q.put_nowait(work)

    def queue_work(self, work):
        self.q.put_nowait(work)

    def qsize(self):
        return self.q.qsize()

    def set_ridealong(self, ridealongid, work):
        self.ridealong[ridealongid] = work

    def get_ridealong(self, ridealongid):
        if ridealongid in self.ridealong:
            return self.ridealong[ridealongid]
        else:
            LOGGER.warning('ridealong data for surt %s not found', ridealongid)
            return {}

    def del_ridealong(self, ridealongid):
        if ridealongid in self.ridealong:
            del self.ridealong[ridealongid]

    def ridealong_size(self):
        return len(self.ridealong)

    def done(self, worker_count):
        return self.awaiting_work == worker_count and self.q.qsize() == 0

    async def close(self):
        # we got here in a sligthly racy fashion, which occasionally results in hangs
        # work around it a little
        while self.q.qsize() != 0:
            LOGGER.error('Got to scheduler.close and the queue was not empty')
            stats.coroutine_report()
            await asyncio.sleep(30.)
        await self.q.join()

    def save(self, crawler, f):
        # XXX make this more self-describing
        pickle.dump('Put the XXX header here', f)  # XXX date, conf file name, conf file checksum
        pickle.dump(self.ridealong, f)
        pickle.dump(crawler._seeds, f)
        count = self.q.qsize()
        pickle.dump(count, f)
        for _ in range(0, count):
            work = self.q.get_nowait()
            pickle.dump(work, f)

    def load(self, crawler, f):
        header = pickle.load(f)  # XXX check that this is a good header... log it
        self.ridealong = pickle.load(f)
        crawler._seeds = pickle.load(f)
        self.q = asyncio.PriorityQueue()
        count = pickle.load(f)
        for _ in range(0, count):
            work = pickle.load(f)
            self.q.put_nowait(work)

    def dump_frontier(self):
        while True:
            try:
                work = self.q.get_nowait()
            except asyncio.queues.QueueEmpty:
                break
            priority, rand, surt = work
            ridealong = self.get_ridealong(surt)
            print(json.dumps({'priority': priority, 'rand': rand, 'url': ridealong['url'].url}))

    def summarize(self):
        '''
        Print a human-readable summary of what's in the queues
        '''
        print('{} items in the crawl queue'.format(self.q.qsize()))
        print('{} items in the ridealong dict'.format(len(self.ridealong)))

        if self.q.qsize() != len(self.ridealong):
            LOGGER.error('Different counts for queue size and ridealong size')
            q_keys = set()
            try:
                while True:
                    priority, rand, surt = self.q.get_nowait()
                    q_keys.add(surt)
            except asyncio.queues.QueueEmpty:
                pass
            ridealong_keys = set(self.ridealong.keys())
            extra_q = q_keys.difference(ridealong_keys)
            extra_r = ridealong_keys.difference(q_keys)
            if extra_q:
                print('Extra urls in queues and not ridealong')
                print(extra_q)
            if extra_r:
                print('Extra urls in ridealong and not queues')
                print(extra_r)
                for r in extra_r:
                    print('  ', r, self.ridealong[r])
            raise ValueError('cannot continue, I just destroyed the queue')

        priority_count = defaultdict(int)
        netlocs = defaultdict(int)
        for k, v in self.ridealong.items():
            priority_count[v['priority']] += 1
            url = v['url']
            netlocs[url.urlsplit.netloc] += 1

        print('{} different hosts in the queue'.format(len(netlocs)))
        print('Queue counts by priority:')
        for p in sorted(list(priority_count.keys())):
            if priority_count[p] > 0:
                print('  {}: {}'.format(p, priority_count[p]))
        print('Queue counts for top 10 netlocs')
        netloc_order = sorted(netlocs.items(), key=itemgetter(1), reverse=True)[0:10]
        for k, v in netloc_order:
            print('  {}: {}'.format(k, v))

    def update_priority(self, priority, rand):
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

    def memory(self):
        '''
        Return a dict summarizing the scheduler's memory usage
        '''
        q = {}
        q['bytes'] = memory.total_size(self.q)
        q['len'] = self.q.qsize()
        ridealong = {}
        ridealong['bytes'] = memory.total_size(self.ridealong)
        ridealong['len'] = len(self.ridealong)
        next_fetch = {}
        next_fetch['bytes'] = memory.total_size(self.next_fetch)
        next_fetch['len'] = len(self.next_fetch)
        frozen_until = {}
        frozen_until['bytes'] = memory.total_size(self.frozen_until)
        frozen_until['len'] = len(self.frozen_until)
        return {'q': q, 'ridealong': ridealong,
                'next_fetch': next_fetch, 'frozen_until': frozen_until}
