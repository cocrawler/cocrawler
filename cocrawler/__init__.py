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

import asyncio
import uvloop
import logging
import aiohttp
import aiohttp.resolver
import aiohttp.connector
import psutil

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

LOGGER = logging.getLogger(__name__)
__title__ = 'cocrawler'
__author__ = 'Greg Lindahl and others'
__license__ = 'Apache 2.0'
__copyright__ = 'Copyright 2016-2017 Greg Lindahl and others'


class FixupEventLoopPolicy(uvloop.EventLoopPolicy):
    '''
    pytest-asyncio is weird and hijacking new_event_loop is one way to work around that.
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


class Crawler:
    def __init__(self, load=None, no_test=False):
        asyncio.set_event_loop_policy(FixupEventLoopPolicy())
        self.loop = asyncio.get_event_loop()
        self.burner = burner.Burner('parser')
        self.stopping = 0
        self.paused = 0
        self.no_test = no_test
        self.next_minute = time.time() + 60
        self.scheduler = scheduler.Scheduler()
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

        self.robotname, self.ua = useragent.useragent(self.version)

        self.resolver = dns.get_resolver()

        geoip.init()

        proxy = config.read('Fetcher', 'ProxyAll')
        if proxy:
            raise ValueError('proxies not yet supported')

        # TODO: save the kwargs in case we want to make a ProxyConnector deeper down
        self.conn_kwargs = {'use_dns_cache': False, 'resolver': self.resolver,
                            'limit': max(1, self.max_workers//2),
                            'enable_cleanup_closed': True}
        local_addr = config.read('Fetcher', 'LocalAddr')
        if local_addr:
            self.conn_kwargs['local_addr'] = (local_addr, 0)
        self.conn_kwargs['family'] = socket.AF_INET  # XXX config option -- this is ipv4 only

        conn = aiohttp.connector.TCPConnector(**self.conn_kwargs)
        self.connector = conn

        conn_timeout = config.read('Crawl', 'ConnectTimeout')
        if not conn_timeout:
            conn_timeout = None  # docs say 0. is no timeout, docs lie
        cookie_jar = aiohttp.DummyCookieJar()
        self.session = aiohttp.ClientSession(connector=conn, cookie_jar=cookie_jar,
                                             conn_timeout=conn_timeout)

        self.datalayer = datalayer.Datalayer()
        self.robots = robots.Robots(self.robotname, self.session, self.datalayer)

        self.crawllog = config.read('Logging', 'Crawllog')
        if self.crawllog:
            self.crawllogfd = open(self.crawllog, 'a')
        else:
            self.crawllogfd = None

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

        self.warcwriter = warc.setup(self.version, local_addr)

        url_allowed.setup()

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
        self.pause_crawler = os.path.expanduser('~/PAUSECRAWLER.{}'.format(os.getpid()))

        LOGGER.info('Touch %s to stop the crawler.', self.stop_crawler)
        LOGGER.info('Touch %s to pause the crawler.', self.pause_crawler)

    def __del__(self):
        self.connector.close()

    @property
    def seeds(self):
        return self._seeds

    @property
    def qsize(self):
        return self.scheduler.qsize()

    def log_rejected_add_url(self, url):
        if self.rejectedaddurlfd:
            print(url.url, file=self.rejectedaddurlfd)

    def add_url(self, priority, ridealong):
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
        if priority > int(config.read('Crawl', 'MaxDepth')):
            stats.stats_sum('rejected by MaxDepth', 1)
            self.log_rejected_add_url(url)
            return
        if 'skip_seen_url' not in ridealong:
            if self.datalayer.seen_url(url):
                stats.stats_sum('rejected by seen_urls', 1)
                self.log_rejected_add_url(url)
                return
        else:
            del ridealong['skip_seen_url']
        if not url_allowed.url_allowed(url):
            LOGGER.debug('url %s was rejected by url_allow.', url.url)
            stats.stats_sum('rejected by url_allowed', 1)
            self.log_rejected_add_url(url)
            return
        # end allow/deny plugin

        LOGGER.debug('actually adding url %s, surt %s', url.url, url.surt)
        stats.stats_sum('added urls', 1)

        ridealong['priority'] = priority

        # to randomize fetches, and sub-prioritize embeds
        if ridealong.get('embed'):
            rand = 0.0
        else:
            rand = random.uniform(0, 0.99999)

        self.scheduler.set_ridealong(url.surt, ridealong)

        self.scheduler.queue_work((priority, rand, url.surt))

        self.datalayer.add_seen_url(url)
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
        parse.report()
        stats.check(no_test=self.no_test)
        stats.check_collisions()
        if self.crawllogfd:
            self.crawllogfd.close()
        if self.rejectedaddurlfd:
            self.rejectedaddurlfd.close()
        if self.facetlogfd:
            self.facetlogfd.close()
        if self.scheduler.qsize():
            LOGGER.warning('at exit, non-zero qsize=%d', self.scheduler.qsize())
        await self.session.close()

    def _retry_if_able(self, work, ridealong):
        priority, rand, surt = work
        retries_left = ridealong.get('retries_left', 0) - 1
        if retries_left <= 0:
            # XXX jsonlog hard fail
            # XXX remember that this host had a hard fail
            stats.stats_sum('retries completely exhausted', 1)
            self.scheduler.del_ridealong(surt)
            seeds.fail(ridealong, self)
            return
        # XXX jsonlog this soft fail
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
        seed_host = ridealong.get('seed_host', None)

        req_headers, proxy, mock_url, mock_robots = fetcher.apply_url_policies(url, self)

        host_geoip = {}
        if not mock_url:
            entry = await dns.prefetch(url, self.resolver)
            if not entry:
                # fail out, we don't want to do DNS in the robots or page fetch
                self._retry_if_able(work, ridealong)
                return
            addrs, expires, _, host_geoip = entry
            if not host_geoip:
                with stats.record_burn('geoip lookup'):
                    geoip.lookup_all(addrs, host_geoip)
                post_fetch.post_dns(addrs, expires, url, self)

        r = await self.robots.check(url, host_geoip, seed_host, self,
                                    headers=req_headers, proxy=proxy, mock_robots=mock_robots)
        if not r:
            # really, we shouldn't retry a robots.txt rule failure
            # but we do want to retry robots.txt failed to fetch
            self._retry_if_able(work, ridealong)
            return

        f = await fetcher.fetch(url, self.session, max_page_size=self.max_page_size,
                                headers=req_headers, proxy=proxy, mock_url=mock_url)

        json_log = {'kind': 'get', 'url': url.url, 'priority': priority,
                    't_first_byte': f.t_first_byte, 'time': time.time()}
        if seed_host:
            json_log['seed_host'] = seed_host
        if f.is_truncated:
            json_log['truncated'] = f.is_truncated

        if f.last_exception is not None or f.response.status >= 500:
            self._retry_if_able(work, ridealong)
            return

        # success

        self.scheduler.del_ridealong(surt)

        json_log['status'] = f.response.status

        if post_fetch.is_redirect(f.response):
            post_fetch.handle_redirect(f, url, ridealong, priority, host_geoip, json_log, self, seed_host=seed_host)
            # meta-http-equiv-redirect will be dealt with in post_fetch

        if f.response.status == 200:
            await post_fetch.post_200(f, url, priority, host_geoip, seed_host, json_log, self)

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
                # ValueError('no A records found') should not be a mystery
                except Exception as e:
                    # this catches any buggy code that executes in the main thread
                    LOGGER.error('Something bad happened working on %s, it\'s a mystery:\n%s', work[2], e)
                    traceback.print_exc()
                    # falling through causes this work item to get marked done, and we continue

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
                LOGGER.info('control_limit: elapsed = %f, adjusting limit by %+d to %d',
                            elapsed, limit - old_limit, limit)
            else:
                LOGGER.info('control_limit: elapsed = %f', elapsed)

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
        if time.time() > self.next_minute:
            self.next_minute = time.time() + 60
            stats.stats_set('DNS cache size', self.resolver.size())
            stats.report()
            stats.coroutine_report()

    def update_cpu_stats(self):
        elapsedc = time.clock()  # should be since process start
        stats.stats_set('main thread cpu time', elapsedc)

    async def crawl(self):
        '''
        Run the crawler until it's out of work
        '''
        self.control_limit_worker = asyncio.Task(self.control_limit())
        self.workers = [asyncio.Task(self.work()) for _ in range(self.max_workers)]

        # this is now the 'main' coroutine

        if config.read('Multiprocess', 'Affinity'):
            # set the main thread to run on core 0
            p = psutil.Process()
            p.cpu_affinity([p.cpu_affinity().pop(0)])

        while True:
            await asyncio.sleep(1)

            if os.path.exists(self.stop_crawler):
                LOGGER.warning('saw STOPCRAWLER file, stopping crawler and saving queues')
                self.stopping = 1

            if os.path.exists(self.pause_crawler):
                LOGGER.warning('saw PAUSECRAWLER file, pausing crawler')
                self.paused = 1
            elif self.paused:
                LOGGER.warning('saw PAUSECRAWLER file disappear, un-pausing crawler')
                self.paused = 0

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

        self.cancel_workers()

        if self.stopping or config.read('Save', 'SaveAtExit'):
            self.summarize()
            self.datalayer.summarize()
            LOGGER.warning('saving datalayer and queues')
            self.save_all()
            LOGGER.warning('saving done')
