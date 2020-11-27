'''
The actual web crawler
'''

import time
import os
import random
import socket
from pkg_resources import get_distribution, DistributionNotFound
from setuptools_scm import get_version
import json
import traceback
import concurrent
import resource
import ssl

import asyncio
import uvloop
import logging
import aiohttp
import aiohttp.resolver
import aiohttp.connector
import psutil
import certifi

from . import scheduler
from . import stats
from . import seeds
from . import datalayer
from . import robots
from . import parse
from . import fetcher
from . import useragent
from . import burner
from . import url_allowed
from . import post_fetch
from . import config
from . import warc
from . import dns
from . import geoip
from . import memory

LOGGER = logging.getLogger(__name__)
__title__ = 'cocrawler'
__author__ = 'Greg Lindahl and others'
__license__ = 'Apache 2.0'
__copyright__ = 'Copyright 2016-2017 Greg Lindahl and others'


class Crawler:
    def __init__(self, load=None, no_test=False, paused=False):
        self.loop = asyncio.get_event_loop()
        self.burner = burner.Burner('parser')
        self.stopping = False
        self.paused = paused
        self.no_test = no_test
        self.next_minute = 0
        self.next_hour = time.time() + 3600
        self.max_page_size = int(config.read('Crawl', 'MaxPageSize'))
        self.prevent_compression = config.read('Crawl', 'PreventCompression')
        self.upgrade_insecure_requests = config.read('Crawl', 'UpgradeInsecureRequests')
        self.max_workers = int(config.read('Crawl', 'MaxWorkers'))
        self.workers = []

        try:
            # this works for the installed package
            self.version = get_distribution(__name__).version
        except DistributionNotFound:
            # this works for an uninstalled git repo, like in the CI infrastructure
            self.version = get_version(root='..', relative_to=__file__)
        self.warcheader_version = '0.99'

        self.robotname, self.ua = useragent.useragent(self.version)

        self.resolver = dns.get_resolver()

        geoip.init()

        self.conn_kwargs = {'use_dns_cache': False, 'resolver': self.resolver,
                            'limit': max(1, self.max_workers//2),
                            'enable_cleanup_closed': True}
        local_addr = config.read('Fetcher', 'LocalAddr')
        if local_addr:
            self.conn_kwargs['local_addr'] = (local_addr, 0)
        self.conn_kwargs['family'] = socket.AF_INET  # XXX config option -- this is ipv4 only
        self.conn_kwargs['ssl'] = ssl.create_default_context(cafile=certifi.where())
        # see https://bugs.python.org/issue27970 for python not handling missing intermediates

        conn = aiohttp.connector.TCPConnector(**self.conn_kwargs)
        self.connector = conn

        connect_timeout = float(config.read('Crawl', 'ConnectTimeout'))
        page_timeout = float(config.read('Crawl', 'PageTimeout'))
        timeout_kwargs = {}
        if connect_timeout:
            timeout_kwargs['sock_connect'] = connect_timeout
        if page_timeout:
            timeout_kwargs['total'] = page_timeout
        timeout = aiohttp.ClientTimeout(**timeout_kwargs)

        cookie_jar = aiohttp.DummyCookieJar()
        self.session = aiohttp.ClientSession(connector=conn, cookie_jar=cookie_jar,
                                             auto_decompress=False, timeout=timeout)

        self.datalayer = datalayer.Datalayer()
        self.robots = robots.Robots(self.robotname, self.session, self.datalayer)
        self.scheduler = scheduler.Scheduler(self.robots, self.resolver)

        self.crawllog = config.read('Logging', 'Crawllog')
        if self.crawllog:
            self.crawllogfd = open(self.crawllog, 'a')
        else:
            self.crawllogfd = None

        self.frontierlog = config.read('Logging', 'Frontierlog')
        if self.frontierlog:
            self.frontierlogfd = open(self.frontierlog, 'a')
        else:
            self.frontierlogfd = None

        self.rejectedaddurl = config.read('Logging', 'RejectedAddUrllog')
        if self.rejectedaddurl:
            self.rejectedaddurlfd = open(self.rejectedaddurl, 'a')
        else:
            self.rejectedaddurlfd = None

        self.facetlog = config.read('Logging', 'Facetlog')
        if self.facetlog:
            self.facetlogfd = open(self.facetlog, 'a')
        else:
            self.facetlogfd = None

        self.warcwriter = warc.setup(self.version, self.warcheader_version, local_addr)

        url_allowed.setup()
        stats.init()

        if load is not None:
            self.load_all(load)
            LOGGER.info('after loading saved state, work queue is %r urls', self.scheduler.qsize())
            LOGGER.info('at time of loading, stats are')
            stats.report()
        else:
            self._seeds = seeds.expand_seeds_config(self)
            LOGGER.info('after adding seeds, work queue is %r urls', self.scheduler.qsize())
            stats.stats_max('initial seeds', self.scheduler.qsize())

        self.stop_crawler = os.path.expanduser('~/STOPCRAWLER.{}'.format(os.getpid()))
        LOGGER.info('Touch %s to stop the crawler.', self.stop_crawler)

        self.pause_crawler = os.path.expanduser('~/PAUSECRAWLER.{}'.format(os.getpid()))
        LOGGER.info('Touch %s to pause the crawler.', self.pause_crawler)

        self.memory_crawler = os.path.expanduser('~/MEMORYCRAWLER.{}'.format(os.getpid()))
        LOGGER.info('Use %s to debug objects in the crawler.', self.memory_crawler)

        fetcher.establish_filters()

    @property
    def seeds(self):
        return self._seeds

    @property
    def qsize(self):
        return self.scheduler.qsize()

    def log_rejected_add_url(self, url, reason):
        if self.rejectedaddurlfd:
            log_line = {'url': url.url, 'reason': reason}
            print(json.dumps(log_line, sort_keys=True), file=self.rejectedaddurlfd)

    def log_frontier(self, url):
        if self.frontierlogfd:
            print(url.url, file=self.frontierlogfd)

    def add_url(self, priority, ridealong, rand=None):
        # XXX eventually do something with the frag - record as a "javascript-needed" clue

        # XXX optionally generate additional urls plugin here
        # e.g. any amazon url with an AmazonID should add_url() the base product page
        # and a non-homepage should add the homepage
        # and a homepage add should add soft404 detection
        # and ...

        url = ridealong['url']
        if 'seed' in ridealong:
            seeds.seed_from_redir(url)

        # XXX allow/deny plugin modules go here
        if self.robots.check_cached(url) == 'denied':
            reason = 'denied by cached robots'
            stats.stats_sum('add_url '+reason, 1)
            self.log_rejected_add_url(url, reason)
            return

        reason = None

        allowed = url_allowed.url_allowed(url)
        if not allowed:
            reason = 'rejected by url_allowed'
        elif allowed.url != url.url:
            LOGGER.debug('url %s was modified to %s by url_allow.', url.url, allowed.url)
            stats.stats_sum('add_url modified by url_allowed', 1)
            url = allowed
            ridealong['url'] = url

        if reason:
            pass
        elif priority > int(config.read('Crawl', 'MaxDepth')):
            reason = 'rejected by MaxDepth'
        elif 'skip_crawled' not in ridealong and self.datalayer.seen(url):
            reason = 'rejected by crawled'
        elif not self.scheduler.check_budgets(url):
            # the budget is debited here, so it has to be last
            reason = 'rejected by crawl budgets'

        if 'skip_crawled' in ridealong:
            self.log_frontier(url)
        elif not self.datalayer.seen(url):
            self.log_frontier(url)

        if reason:
            stats.stats_sum('add_url '+reason, 1)
            self.log_rejected_add_url(url, reason)
            LOGGER.debug('add_url no, reason %s url %s', reason, url.url)
            return

        if 'skip_crawled' in ridealong:
            del ridealong['skip_crawled']

        # end allow/deny plugin

        LOGGER.debug('actually adding url %s, surt %s', url.url, url.surt)
        stats.stats_sum('added urls', 1)

        ridealong['priority'] = priority

        # to randomize fetches
        # already set for a freeredir
        # could be used to sub-prioritize embeds
        if rand is None:
            rand = random.uniform(0, 0.99999)

        self.scheduler.set_ridealong(url.surt, ridealong)

        self.scheduler.queue_work((priority, rand, url.surt))

        self.datalayer.add_seen(url)
        return 1

    def cancel_workers(self):
        for w in self.workers:
            if not w.done():
                w.cancel()
        cw = self.control_limit_worker
        if cw and not cw.done():
            cw.cancel()

    async def close(self):
        stats.report()
        memory.print_summary(self.memory_crawler)
        parse.report()
        stats.check(no_test=self.no_test)
        stats.check_collisions()
        if self.crawllogfd:
            self.crawllogfd.close()
        if self.rejectedaddurlfd:
            self.rejectedaddurlfd.close()
        if self.facetlogfd:
            self.facetlogfd.close()
        if self.frontierlogfd:
            self.frontierlogfd.close()
        if self.warcwriter is not None:
            del self.warcwriter
            self.warcwriter = None
        if self.robots is not None:
            del self.robots
            self.robots = None
        if self.scheduler.qsize():
            LOGGER.warning('at exit, non-zero qsize=%d', self.scheduler.qsize())
        await self.session.close()
        await self.connector.close()

    def _retry_if_able(self, work, ridealong, json_log, stats_prefix=''):
        priority, rand, surt = work
        retries_left = ridealong.get('retries_left', 0) - 1
        if json_log:
            json_log['retries_left'] = retries_left
        if retries_left <= 0:
            stats.stats_sum(stats_prefix+'retries completely exhausted', 1)
            self.scheduler.del_ridealong(surt)
            seeds.fail(ridealong, self, json_log)
            return
        ridealong['retries_left'] = retries_left
        self.scheduler.set_ridealong(surt, ridealong)
        # increment random so that we don't immediately retry
        extra = random.uniform(0, 0.2)
        priority, rand = self.scheduler.update_priority(priority, rand+extra)
        ridealong['priority'] = priority
        self.scheduler.requeue_work((priority, rand, surt))
        return

    async def fetch_and_process(self, work):
        '''
        Fetch and process a single url.
        '''
        priority, rand, surt = work

        # when we're in the dregs of retried urls with high rand, don't exceed priority+1
        stats.stats_set('priority', priority+min(rand, 0.99))

        ridealong = self.scheduler.get_ridealong(surt)
        if 'url' not in ridealong:
            raise ValueError('missing ridealong for surt '+surt)
        url = ridealong['url']
        seed_host = ridealong.get('seed_host')
        if seed_host and ridealong.get('seed'):
            robots_seed_host = seed_host
        else:
            robots_seed_host = None

        prefetch_dns, get_kwargs = fetcher.apply_url_policies(url, self)

        json_log = {'kind': 'get', 'url': url.url, 'priority': priority, 'time': time.time()}
        if get_kwargs['proxy']:
            json_log['proxy'] = True
        if seed_host:
            json_log['seed_host'] = seed_host

        host_geoip = {}
        dns_entry = None
        if prefetch_dns:
            dns_entry = await dns.prefetch(url, self.resolver)
            if dns_entry:
                json_log['ip'] = dns.entry_to_as(dns_entry)
            else:
                # fail out: we don't want to do DNS in the robots or page fetch
                self._retry_if_able(work, ridealong, json_log)
                json_log['fail'] = 'no dns info'
                if self.crawllogfd:
                    print(json.dumps(json_log, sort_keys=True), file=self.crawllogfd)
                return
            addrs, expires, _, host_geoip = dns_entry
            if not host_geoip:
                with stats.record_burn('geoip lookup'):
                    geoip.lookup_all(addrs, host_geoip)
                post_fetch.post_dns(addrs, expires, url, self)

        r = await self.robots.check(url, dns_entry=dns_entry, seed_host=robots_seed_host,
                                    crawler=self, get_kwargs=get_kwargs)
        if r != 'allowed':
            if r == 'no robots':
                json_log['fail'] = 'no robots'
            else:
                json_log['fail'] = 'robots denied'
            self._retry_if_able(work, ridealong, json_log, stats_prefix='robots ')
            if self.crawllogfd:
                print(json.dumps(json_log, sort_keys=True), file=self.crawllogfd)
            return

        f = await fetcher.fetch(url, self.session, max_page_size=self.max_page_size,
                                get_kwargs=get_kwargs)

        if f.is_truncated:
            json_log['truncated'] = f.is_truncated
        if f.response:
            json_log['status'] = f.response.status
        if f.last_exception:
            json_log['exception'] = f.last_exception
        if f.t_first_byte is not None:
            json_log['t_first_byte'] = f.t_first_byte
        if f.ip is not None:
            json_log['ip'] = f.ip
        elif 'ip' in json_log:
            stats.stats_sum('fetch ip is from dns', 1)

        if post_fetch.should_retry(f):
            self._retry_if_able(work, ridealong, json_log)
            if self.crawllogfd:
                print(json.dumps(json_log, sort_keys=True), file=self.crawllogfd)
            return

        self.scheduler.del_ridealong(surt)

        # from here down we want to jsonlog and stats everything

        if f.response.status >= 400:
            json_log['unretryable_4xx'] = True
            stats.stats_sum('unretryable_4xx', 1)
        elif f.response.status >= 300:
            if not post_fetch.is_redirect(f.response):
                json_log['unretryable_3xx'] = True,
                stats.stats_sum('unretryable_3xx', 1)
        elif f.response.status < 200:
            json_log['unretryable_1xx'] = True,
            stats.stats_sum('unretryable_1xx', 1)

        if 200 <= f.response.status < 300:
            await post_fetch.post_2xx(f, url, ridealong, priority, host_geoip, json_log, self)
        elif post_fetch.is_redirect(f.response):
            post_fetch.handle_redirect(f, url, ridealong, priority, host_geoip, json_log, self, rand=rand)
            # meta-http-equiv-redirect will be dealt with in post_fetch
        else:
            seeds.fail(ridealong, self, json_log)

        LOGGER.debug('size of work queue now stands at %r urls', self.scheduler.qsize())
        LOGGER.debug('size of ridealong now stands at %r urls', self.scheduler.ridealong_size())
        stats.stats_set('queue size', self.scheduler.qsize())
        stats.stats_max('max queue size', self.scheduler.qsize())
        stats.stats_set('ridealong size', self.scheduler.ridealong_size())

        if self.crawllogfd:
            print(json.dumps(json_log, sort_keys=True), file=self.crawllogfd)

    async def work(self):
        '''
        Process queue items until we run out.
        '''
        try:
            while True:
                work = await self.scheduler.get_work()

                try:
                    await self.fetch_and_process(work)
                except concurrent.futures._base.CancelledError:  # seen with ^C
                    pass
                except Exception as e:
                    # this catches any buggy code that executes in the main thread
                    LOGGER.error('Something bad happened working on %s, it\'s a mystery:\n%s', work[2], e)
                    traceback.print_exc()
                    # falling through causes this work item to get marked done, and we continue
                    # note that this leaks the ridealong
                self.scheduler.work_done()

                if self.stopping:
                    raise asyncio.CancelledError

                if self.paused:
                    with stats.coroutine_state('paused'):
                        while self.paused:
                            await asyncio.sleep(1)

        except asyncio.CancelledError:
            pass

    async def control_limit(self):
        '''
        Worker dedicated to managing how busy we let the network get
        '''
        last = time.time()
        await asyncio.sleep(1.0)
        limit = self.max_workers//2
        dominos = 0
        undominos = 0
        while True:
            await asyncio.sleep(1.0)
            t = time.time()
            elapsed = t - last
            old_limit = limit

            if elapsed < 1.03:
                dominos += 1
                undominos = 0
                if dominos > 2:
                    # one action per 3 seconds of stability
                    limit += 1
                    dominos = 0
            else:
                dominos = 0
                if elapsed > 5.0:
                    # always act on tall spikes
                    limit -= max((limit * 5) // 100, 1)  # 5%
                    undominos = 0
                elif elapsed > 1.1:
                    undominos += 1
                    if undominos > 1:
                        # only act if the medium spike is wider than 1 cycle
                        # (note: these spikes are caused by garbage collection)
                        limit -= max(limit // 100, 1)  # 1%
                        undominos = 0
                else:
                    undominos = 0
            limit = min(limit, self.max_workers)
            limit = max(limit, 1)

            self.connector._limit = limit  # private instance variable
            stats.stats_set('network limit', limit)
            last = t

            if limit != old_limit:
                LOGGER.debug('control_limit: elapsed = %f, adjusting limit by %+d to %d',
                             elapsed, limit - old_limit, limit)
            else:
                LOGGER.debug('control_limit: elapsed = %f', elapsed)

    def summarize(self):
        self.scheduler.summarize()

    def save(self, f):
        self.scheduler.save(self, f, )

    def load(self, f):
        self.scheduler.load(self, f)

    def get_savefilename(self):
        savefile = config.read('Save', 'Name') or 'cocrawler-save-$$'
        savefile = savefile.replace('$$', str(os.getpid()))
        savefile = os.path.expanduser(os.path.expandvars(savefile))
        if os.path.exists(savefile) and not config.read('Save', 'Overwrite'):
            count = 1
            while os.path.exists(savefile + '.' + str(count)):
                count += 1
            savefile = savefile + '.' + str(count)
        return savefile

    def save_all(self):
        savefile = self.get_savefilename()
        with open(savefile, 'wb') as f:
            self.save(f)
            self.datalayer.save(f)
            stats.save(f)

    def load_all(self, filename):
        with open(filename, 'rb') as f:
            self.load(f)
            self.datalayer.load(f)
            stats.load(f)

    def minute(self):
        '''
        print interesting stuff, once a minute
        '''
        if time.time() < self.next_minute:
            return

        self.next_minute = time.time() + 60
        stats.stats_set('DNS cache size', self.resolver.size())
        ru = resource.getrusage(resource.RUSAGE_SELF)
        vmem = (ru[2])/1000000.  # gigabytes
        stats.stats_set('main thread vmem', vmem)
        stats.report()
        stats.coroutine_report()
        memory.print_summary(self.memory_crawler)

    def hour(self):
        '''Do something once per hour'''
        if time.time() < self.next_hour:
            return

        self.next_hour = time.time() + 3600
        pass

    def update_cpu_stats(self):
        elapsedc = time.process_time()  # should be since process start
        stats.stats_set('main thread cpu time', elapsedc)

    async def crawl(self):
        '''
        Run the crawler until it's out of work
        '''
        self.minute()  # print pre-start stats

        self.control_limit_worker = asyncio.Task(self.control_limit())
        self.workers = [asyncio.Task(self.work()) for _ in range(self.max_workers)]

        # this is now the 'main' coroutine

        if config.read('Multiprocess', 'Affinity'):
            p = psutil.Process()
            if hasattr(p, 'cpu_affinity'):  # MacOS does not
                # set the main thread to run on core 0
                cpu = p.cpu_affinity().pop(0)
                p.cpu_affinity([cpu])
                LOGGER.info('setting cpu affinity of main thread to core %d', cpu)
            else:
                pass  # already sent a warning in burner constructor

        while True:
            await asyncio.sleep(1)

            if not self.stopping and os.path.exists(self.stop_crawler):
                LOGGER.warning('saw STOPCRAWLER file, stopping crawler and saving queues')
                self.stopping = True

            if not self.paused and os.path.exists(self.pause_crawler):
                LOGGER.warning('saw PAUSECRAWLER file, pausing crawler')
                self.paused = True
            elif self.paused and not os.path.exists(self.pause_crawler):
                LOGGER.warning('saw PAUSECRAWLER file disappear, un-pausing crawler')
                self.paused = False

            self.workers = [w for w in self.workers if not w.done()]
            LOGGER.debug('%d workers remain', len(self.workers))
            if len(self.workers) == 0:
                # this triggers if we've exhausted our url budget and all workers cancel themselves
                # queue will likely not be empty in this case
                LOGGER.warning('all workers exited, finishing up.')
                break

            if self.scheduler.done(len(self.workers)):
                # this is a little racy with how awaiting work is set and the queue is read
                # while we're in this join we aren't looking for STOPCRAWLER etc
                LOGGER.warning('all workers appear idle, queue appears empty, executing join')
                await self.scheduler.close()
                break

            self.update_cpu_stats()
            self.minute()
            self.hour()

        self.cancel_workers()

        if self.stopping or config.read('Save', 'SaveAtExit'):
            self.summarize()
            self.datalayer.summarize()
            LOGGER.warning('saving datalayer and queues')
            self.save_all()
            LOGGER.warning('saving done')
