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

import asyncio
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
from . import cookies
from . import post_fetch
from . import config
from .warc import CCWARCWriter
from . import dns

LOGGER = logging.getLogger(__name__)
__title__ = 'cocrawler'
__author__ = 'Greg Lindahl and others'
__license__ = 'Apache 2.0'
__copyright__ = 'Copyright 2016-2017 Greg Lindahl and others'


class Crawler:
    def __init__(self, loop, load=None, no_test=False):
        self.loop = loop
        self.burner = burner.Burner(loop, 'parser')
        self.stopping = 0
        self.paused = 0
        self.no_test = no_test
        self.next_minute = time.time() + 60

        try:
            # this works for the installed package
            self.version = get_distribution(__name__).version
        except DistributionNotFound:
            # this works for an uninstalled git repo, like in the CI infrastructure
            self.version = get_version(root='..', relative_to=__file__)

        self.robotname, self.ua = useragent.useragent(self.version)

        ns = config.read('Fetcher', 'Nameservers')
        resolver = dns.get_resolver_wrapper(loop=loop, nameservers=ns)

        proxy = config.read('Fetcher', 'ProxyAll')
        if proxy:
            raise ValueError('proxies not yet supported')

        local_addr = config.read('Fetcher', 'LocalAddr')
        # TODO: save the kwargs in case we want to make a ProxyConnector deeper down
        #self.conn_kwargs = {'use_dns_cache': True, 'resolver': resolver,
        #                    'ttl_dns_cache': 3600*8,  # this is a maximum TTL XXX need to call .clear occasionally
        self.conn_kwargs = {'use_dns_cache': False, 'resolver': resolver}

        if local_addr:
            self.conn_kwargs['local_addr'] = local_addr
        self.conn_kwargs['family'] = socket.AF_INET  # XXX config option
        conn = aiohttp.connector.TCPConnector(**self.conn_kwargs)
        self.connector = conn
        if (config.read('Crawl', 'CookieJar') or '') == 'Defective':
            cookie_jar = cookies.DefectiveCookieJar()
        else:
            cookie_jar = None  # which means a normal cookie jar
        self.session = aiohttp.ClientSession(loop=loop, connector=conn, cookie_jar=cookie_jar)

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

        warcall = config.read('WARC', 'WARCAll')
        if warcall is not None and warcall:
            max_size = config.read('WARC', 'WARCMaxSize')
            prefix = config.read('WARC', 'WARCPrefix')
            subprefix = config.read('WARC', 'WARCSubPrefix')
            description = config.read('WARC', 'WARCDescription')
            creator = config.read('WARC', 'WARCCreator')
            operator = config.read('WARC', 'WARCOperator')
            self.warcwriter = CCWARCWriter(prefix, max_size, subprefix=subprefix)  # XXX get_serial lacks a default
            self.warcwriter.create_default_info(self.version, local_addr,
                                                description=description, creator=creator, operator=operator)
        else:
            self.warcwriter = None

        scheduler.configure()

        if load is not None:
            self.load_all(load)
            LOGGER.info('after loading saved state, work queue is %r urls', scheduler.qsize())
            LOGGER.info('at time of loading, stats are')
            stats.report()
        else:
            self._seeds = seeds.expand_seeds(config.read('Seeds'))
            for s in self._seeds:
                self.add_url(1, s, seed=True)
            LOGGER.info('after adding seeds, work queue is %r urls', scheduler.qsize())
            stats.stats_max('initial seeds', scheduler.qsize())

        url_allowed.setup(self._seeds)

        self.max_workers = int(config.read('Crawl', 'MaxWorkers'))

        self.workers = []

        LOGGER.info('Touch ~/STOPCRAWLER.%d to stop the crawler.', os.getpid())
        LOGGER.info('Touch ~/PAUSECRAWLER.%d to pause the crawler.', os.getpid())

    def __del__(self):
        self.connector.close()

    @property
    def seeds(self):
        return self._seeds

    @property
    def qsize(self):
        return scheduler.qsize()

    def log_rejected_add_url(self, url):
        if self.rejectedaddurlfd:
            print(url.url, file=self.rejectedaddurlfd)

    def add_url(self, priority, url, seed=False, seedredirs=None):
        # XXX eventually do something with the frag - record as a "javascript-needed" clue

        # XXX optionally generate additional urls plugin here
        # e.g. any amazon url with an AmazonID should add_url() the base product page
        # and a non-homepage should add the homepage
        # and a homepage add should add soft404 detection
        # and ...

        # XXX allow/deny plugin modules go here
        if priority > int(config.read('Crawl', 'MaxDepth')):
            stats.stats_sum('rejected by MaxDepth', 1)
            self.log_rejected_add_url(url)
            return
        if self.datalayer.seen_url(url):
            stats.stats_sum('rejected by seen_urls', 1)
            self.log_rejected_add_url(url)
            return
        if not seed and not url_allowed.url_allowed(url):
            LOGGER.debug('url %s was rejected by url_allow.', url.url)
            stats.stats_sum('rejected by url_allowed', 1)
            self.log_rejected_add_url(url)
            return
        # end allow/deny plugin

        LOGGER.debug('actually adding url %s, surt %s', url.url, url.surt)
        if seed:
            stats.stats_sum('added seeds', 1)
        else:
            stats.stats_sum('added urls', 1)

        work = {'url': url, 'priority': priority}
        if seed:
            work['seed'] = True

        # to randomize fetches, and sub-prioritize embeds
        if work.get('embed'):
            rand = 0.0
        else:
            rand = random.uniform(0, 0.99999)

        scheduler.set_ridealong(url.surt, work)

        scheduler.queue_work((priority, rand, url.surt))

        self.datalayer.add_seen_url(url)
        return 1

    def cancel_workers(self):
        for w in self.workers:
            if not w.done():
                w.cancel()

    def close(self):
        stats.report()
        parse.report()
        stats.check(no_test=self.no_test)
        self.session.close()
        if self.crawllogfd:
            self.crawllogfd.close()
        if scheduler.qsize():
            LOGGER.warning('at exit, non-zero qsize=%d', scheduler.qsize())

    async def fetch_and_process(self, work):
        '''
        Fetch and process a single url.
        '''
        priority, rand, surt = work

        # when we're in the dregs of retried urls with high rand, don't exceed priority+1
        stats.stats_set('priority', priority+min(rand, 0.99))

        ridealong = scheduler.get_ridealong(surt)
        url = ridealong['url']
        tries = ridealong.get('tries', 0)
        maxtries = config.read('Crawl', 'MaxTries')

        req_headers, proxy, mock_url, mock_robots = fetcher.apply_url_policies(url, self.ua)

        with stats.coroutine_state('fetching/checking robots'):
            r = await self.robots.check(url, headers=req_headers, proxy=proxy, mock_robots=mock_robots)
        if not r:
            # XXX there are 2 kinds of fail, no robots data and robots denied. robotslog has the full details.
            # XXX treat 'no robots data' as a soft failure?
            # XXX log more particular robots fail reason here
            json_log = {'type': 'get', 'url': url.url, 'priority': priority,
                        'status': 'robots', 'time': time.time()}
            if self.crawllogfd:
                print(json.dumps(json_log, sort_keys=True), file=self.crawllogfd)
            scheduler.del_ridealong(surt)
            return

        f = await fetcher.fetch(url, self.session,
                                headers=req_headers, proxy=proxy, mock_url=mock_url)

        json_log = {'type': 'get', 'url': url.url, 'priority': priority,
                    't_first_byte': f.t_first_byte, 'time': time.time()}
        if tries:
            json_log['retry'] = tries

        if f.last_exception is not None or f.response.status >= 500:
            tries += 1
            if tries > maxtries:
                # XXX jsonlog hard fail
                # XXX remember that this host had a hard fail
                stats.stats_sum('tries completely exhausted', 1)
                scheduler.del_ridealong(surt)
                return
            # XXX jsonlog this soft fail?
            ridealong['tries'] = tries
            ridealong['priority'] = priority
            scheduler.set_ridealong(surt, ridealong)
            # increment random so that we don't immediately retry
            extra = random.uniform(0, 0.2)
            priority, rand = scheduler.update_priority(priority, rand+extra)
            scheduler.requeue_work((priority, rand, surt))
            return

        scheduler.del_ridealong(surt)

        json_log['status'] = f.response.status

        if post_fetch.is_redirect(f.response):
            post_fetch.handle_redirect(f, url, ridealong, priority, json_log, self)

        if f.response.status == 200:
            await post_fetch.post_200(f, url, priority, json_log, self)

        LOGGER.debug('size of work queue now stands at %r urls', scheduler.qsize())
        stats.stats_set('queue size', scheduler.qsize())
        stats.stats_max('max queue size', scheduler.qsize())

        if self.crawllogfd:
            print(json.dumps(json_log, sort_keys=True), file=self.crawllogfd)

    async def work(self):
        '''
        Process queue items until we run out.
        '''
        try:
            while True:
                work = await scheduler.get_work()

                try:
                    await self.fetch_and_process(work)
                except Exception as e:
                    # this catches any buggy code that executes in the main thread
                    LOGGER.error('Something bad happened somewhere, it\'s a mystery: %s', e)
                    traceback.print_exc()
                    # falling through causes this work item to get marked done, and we continue

                scheduler.work_done()

                if self.stopping:
                    raise asyncio.CancelledError

                if self.paused:
                    with stats.coroutine_state('paused'):
                        while self.paused:
                            await asyncio.sleep(1)

        except asyncio.CancelledError:
            pass

    def summarize(self):
        scheduler.summarize()

    def save(self, f):
        scheduler.save(self, f, )

    def load(self, f):
        scheduler.load(self, f)

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
        print interesting stuf, once a minute
        '''
        if time.time() > self.next_minute:
            self.next_minute = time.time() + 60
            stats.report()

    def update_cpu_stats(self):
        elapsedc = time.clock()  # should be since process start
        stats.stats_set('main thread cpu time', elapsedc)

    async def crawl(self):
        '''
        Run the crawler until it's out of work
        '''
        self.workers = [asyncio.Task(self.work(), loop=self.loop) for _ in range(self.max_workers)]

        # this is now the 'main' coroutine

        if config.read('Multiprocess', 'Affinity'):
            # set the main thread to run on core 0
            p = psutil.Process()
            p.cpu_affinity([0])

        while True:
            await asyncio.sleep(1)

            if os.path.exists(os.path.expanduser('~/STOPCRAWLER.{}'.format(os.getpid()))):
                LOGGER.warning('saw STOPCRAWLER file, stopping crawler and saving queues')
                self.stopping = 1

            if os.path.exists(os.path.expanduser('~/PAUSECRAWLER.{}'.format(os.getpid()))):
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

            if scheduler.done(len(self.workers)):
                # this is a little racy with how awaiting work is set and the queue is read
                # while we're in this join we aren't looking for STOPCRAWLER etc
                LOGGER.warning('all workers appear idle, queue appears empty, executing join')
                await scheduler.close()
                break

            self.update_cpu_stats()
            self.minute()

            # XXX clear the DNS cache every few hours; currently the
            # in-memory one is kept for the entire crawler run

        self.cancel_workers()

        if self.stopping or config.read('Save', 'SaveAtExit'):
            self.summarize()
            self.datalayer.summarize()
            LOGGER.warning('saving datalayer and queues')
            self.save_all()
            LOGGER.warning('saving done')
