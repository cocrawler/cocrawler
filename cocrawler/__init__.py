'''
The actual web crawler
'''

import cgi
import json
import time
import os
from functools import partial
import pickle
from collections import defaultdict
from operator import itemgetter
import random
import socket
from pkg_resources import get_distribution, DistributionNotFound
from setuptools_scm import get_version

import asyncio
import logging
import aiohttp
import aiohttp.resolver
import aiohttp.connector
import psutil

from . import stats
from . import seeds
from . import datalayer
from . import robots
from . import parse
from . import fetcher
from . import useragent
from . import urls
from . import burner
from . import url_allowed
from . import cookies
from .warc import CCWARCWriter

LOGGER = logging.getLogger(__name__)

__title__ = 'cocrawler'
__author__ = 'Greg Lindahl and others'
__license__ = 'Apache 2.0'
__copyright__ = 'Copyright 2016-2017 Greg Lindahl and others'


# aiohttp.ClientReponse lacks this method, so...
def is_redirect(response):
    return response.status in (300, 301, 302, 303, 307)


class Crawler:
    def __init__(self, loop, config, load=None, no_test=False):
        self.config = config
        self.loop = loop
        self.burner = burner.Burner(config, loop, 'parser')
        self.burner_parseinburnersize = int(self.config['Multiprocess']['ParseInBurnerSize'])
        self.stopping = 0
        self.paused = 0
        self.no_test = no_test
        self.next_minute = 0

        try:
            # this works for the installed package
            self.version = get_distribution(__name__).version
        except DistributionNotFound:
            # this works for an uninstalled git repo, like in the CI infrastructure
            self.version = get_version(root='..', relative_to=__file__)

        self.robotname, self.ua = useragent.useragent(config, self.version)

        ns = config['Fetcher'].get('Nameservers')
        if ns:
            resolver = aiohttp.resolver.AsyncResolver(nameservers=ns)
        else:
            resolver = None

        proxy = config['Fetcher'].get('ProxyAll')
        if proxy:
            raise ValueError('proxies not yet supported')

        local_addr = config['Fetcher'].get('LocalAddr')
        # TODO: if local_addr is a list, make up an array of TCPConnecter objects, and rotate
        # TODO: save the kwargs in case we want to make a ProxyConnector deeper down
        self.conn_kwargs = {'use_dns_cache': True, 'resolver': resolver, 'limit': None}
        if local_addr:
            self.conn_kwargs['local_addr'] = local_addr
        self.conn_kwargs['family'] = socket.AF_INET  # XXX config option
        conn = aiohttp.connector.TCPConnector(**self.conn_kwargs)
        self.connector = conn
        if config['Crawl'].get('CookieJar', '') == 'Defective':
            cookie_jar = cookies.DefectiveCookieJar()
        else:
            cookie_jar = None  # which means a normal cookie jar
        self.session = aiohttp.ClientSession(loop=loop, connector=conn, cookie_jar=cookie_jar,
                                             headers={'User-Agent': self.ua})

        self.q = asyncio.PriorityQueue(loop=self.loop)
        self.ridealong = {}
        self.ridealongmaxid = 1  # XXX switch this to using url_canon as the id

        self.datalayer = datalayer.Datalayer(config)
        self.robots = robots.Robots(self.robotname, self.session, self.datalayer, config)

        self.crawllog = config['Logging'].get('Crawllog')
        if self.crawllog:
            self.crawllogfd = open(self.crawllog, 'a')
        else:
            self.crawllogfd = None

        self.rejectedaddurl = config['Logging'].get('RejectedAddUrllog')
        if self.rejectedaddurl:
            self.rejectedaddurlfd = open(self.rejectedaddurl, 'a')
        else:
            self.rejectedaddurlfd = None

        self.facetlog = config['Logging'].get('Facetlog')
        if self.facetlog:
            self.facetlogfd = open(self.facetlog, 'a')
        else:
            self.facetlogfd = None

        if self.config['WARC'].get('WARCAll', False):
            max_size = self.config['WARC']['WARCMaxSize']
            prefix = self.config['WARC']['WARCPrefix']
            subprefix = self.config['WARC'].get('WARCSubPrefix')
            description = self.config['WARC'].get('WARCDescription')
            creator = self.config['WARC'].get('WARCCreator')
            operator = self.config['WARC'].get('WARCOperator')
            self.warcwriter = CCWARCWriter(prefix, max_size, subprefix=subprefix)  # XXX get_serial lacks a default
            self.warcwriter.create_default_info(self.version, local_addr,
                                                description=description, creator=creator, operator=operator)
        else:
            self.warcwriter = None

        if load is not None:
            self.load_all(load)
            LOGGER.info('after loading saved state, work queue is %r urls', self.q.qsize())
        else:
            self._seeds = seeds.expand_seeds(self.config.get('Seeds', {}))
            for s in self._seeds:
                self.add_url(1, s, seed=True)
            LOGGER.info('after adding seeds, work queue is %r urls', self.q.qsize())
            stats.stats_max('initial seeds', self.q.qsize())

        url_allowed.setup(self._seeds, config)

        self.max_workers = int(self.config['Crawl']['MaxWorkers'])
        self.remaining_url_budget = int(self.config['Crawl'].get('MaxCrawledUrls', 0)) or None  # 0 => None
        self.awaiting_work = 0

        self.workers = []

        LOGGER.info('Touch ~/STOPCRAWLER.%d to stop the crawler.', os.getpid())
        LOGGER.info('Touch ~/PAUSECRAWLER.%d to pause the crawler.', os.getpid())

    @property
    def seeds(self):
        return self._seeds

    @property
    def qsize(self):
        return self.q.qsize()

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
        if priority > int(self.config['Crawl']['MaxDepth']):
            stats.stats_sum('rejected by MaxDepth', 1)
            self.log_rejected_add_url(url)
            return
        if self.datalayer.seen_url(url):
            stats.stats_sum('rejected by seen_urls', 1)
            self.log_rejected_add_url(url)
            return
        if not seed and not url_allowed.url_allowed(url):
            LOGGER.debug('url %r was rejected by url_allow.', url)
            stats.stats_sum('rejected by url_allowed', 1)
            self.log_rejected_add_url(url)
            return
        # end allow/deny plugin

        LOGGER.debug('actually adding url %r', url.url)
        if seed:
            stats.stats_sum('added seeds', 1)
        else:
            stats.stats_sum('added urls', 1)

        work = {'url': url, 'priority': priority}
        if seed:
            work['seed'] = True
        self.ridealong[str(self.ridealongmaxid)] = work

        # to randomize fetches, and sub-prioritize embeds
        if work.get('embed'):
            rand = 0.0
        else:
            rand = random.uniform(0, 0.99999)

        self.q.put_nowait((priority, rand, str(self.ridealongmaxid)))
        self.ridealongmaxid += 1

        self.datalayer.add_seen_url(url)
        return 1

    def cancel_workers(self):
        for w in self.workers:
            if not w.done():
                w.cancel()

    def close(self):
        stats.report()
        parse.report()
        stats.check(self.config, no_test=self.no_test)
        self.session.close()
        if self.crawllogfd:
            self.crawllogfd.close()
        if self.q.qsize():
            LOGGER.error('non-zero exit qsize=%d', self.q.qsize())
            stats.exitstatus = 1

    async def fetch_and_process(self, work):
        '''
        Fetch and process a single url.
        '''
        priority, rand, ra = work
        stats.stats_fixed('priority', priority+rand)
        ridealong = self.ridealong[ra]
        url = ridealong['url']
        tries = ridealong.get('tries', 0)
        maxtries = self.config['Crawl']['MaxTries']

        req_headers, proxy, mock_url, mock_robots = fetcher.apply_url_policies(url, self.config)

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
            del self.ridealong[ra]
            return

        f = await fetcher.fetch(url, self.session, self.config,
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
                del self.ridealong[ra]
                return
            # XXX jsonlog this soft fail?
            ridealong['tries'] = tries
            ridealong['priority'] = priority
            self.ridealong[ra] = ridealong
            # increment random so that we don't immediately retry
            extra = random.uniform(0, 0.5)
            self.q.put_nowait((priority, rand+extra, ra))
            return

        del self.ridealong[ra]

        json_log['status'] = f.response.status

        if is_redirect(f.response):
            resp_headers = f.response.headers
            location = resp_headers.get('location')
            if location is None:
                LOGGER.info('%d redirect for %s has no Location: header', f.response.status, url.url)
                # XXX this raise causes "ERROR:asyncio:Task exception was never retrieved"
                raise ValueError(url.url + ' sent a redirect with no Location: header')
            next_url = urls.URL(location, urljoin=url)

            kind = urls.special_redirect(url, next_url)
            if kind is not None:
                if 'seed' in ridealong:
                    prefix = 'redirect seed'
                else:
                    prefix = 'redirect'
                stats.stats_sum(prefix+' '+kind, 1)

            if kind is None:
                pass
            elif kind == 'same':
                LOGGER.info('attempted redirect to myself: %s to %s', url.url, next_url.url)
                if 'Set-Cookie' not in resp_headers:
                    LOGGER.info('redirect to myself had no cookies.')
                    # XXX try swapping www/not-www? or use a non-crawler UA.
                    # looks like some hosts have extra defenses on their redir servers!
                else:
                    # XXX we should use a cookie jar with this domain?
                    pass
                # fall through; will fail seen-url test in addurl
            else:
                #LOGGER.info('special redirect of type %s for url %s', kind, url.url)
                # XXX push this info onto a last-k for the host
                # to be used pre-fetch to mutate urls we think will redir
                pass

            priority += 1
            json_log['redirect'] = next_url.url

            kwargs = {}
            if 'seed' in ridealong:
                if 'seedredirs' in ridealong:
                    ridealong['seedredirs'] += 1
                else:
                    ridealong['seedredirs'] = 1
                if ridealong['seedredirs'] > self.config['Seeds'].get('SeedRedirsCount', 0):
                    del ridealong['seed']
                    del ridealong['seedredirs']
                else:
                    kwargs['seed'] = ridealong['seed']
                    kwargs['seedredirs'] = ridealong['seedredirs']
                    if self.config['Seeds'].get('SeedRedirsFree'):
                        priority -= 1
                    json_log['seedredirs'] = ridealong['seedredirs']

            if self.add_url(priority+1, next_url, **kwargs):  # XXX add more policy regarding priorities
                json_log['found_new_links'] = 1
            # fall through to json logging

        # if 200, parse urls out of body
        if f.response.status == 200:
            resp_headers = f.response.headers
            content_type = resp_headers.get('content-type', 'None')
            # sometimes content_type comes back multiline. whack it with a wrench.
            content_type = content_type.replace('\r', '\n').partition('\n')[0]
            if content_type:
                content_type, _ = cgi.parse_header(content_type)
            else:
                content_type = 'Unknown'
            LOGGER.debug('url %r came back with content type %r', url.url, content_type)
            json_log['content_type'] = content_type
            stats.stats_sum('content-type=' + content_type, 1)
            if self.warcwriter is not None:
                self.warcwriter.write_request_response_pair(url.url, req_headers, f.response.raw_headers, f.body_bytes)  # XXX digest??

            if content_type == 'text/html':
                try:
                    with stats.record_burn('response.text() decode', url=url):
                        body = await f.response.text()  # do not use encoding found in the headers -- policy
                        # XXX consider using 'ascii' for speed, if all we want to do is regex in it
                except (UnicodeDecodeError, LookupError):
                    # LookupError: .text() guessed an encoding that decode() won't understand (wut?)
                    # XXX if encoding was in header, maybe I should use it here?
                    # XXX can get additional exceptions here, broken tcp connect etc. see list in fetcher
                    body = f.body_bytes.decode(encoding='utf-8', errors='replace')

                # headers is a funky object that's allergic to getting pickled.
                # let's make something more boring
                # XXX get rid of this for the one in warc?
                resp_headers_list = []
                for k, v in resp_headers.items():
                    resp_headers_list.append((k.lower(), v))

                if len(body) > self.burner_parseinburnersize:
                    links, embeds, sha1, facets = await self.burner.burn(
                        partial(parse.do_burner_work_html, body, f.body_bytes, resp_headers_list, url=url),
                        url=url)
                else:
                    with stats.coroutine_state('await main thread parser'):
                        links, embeds, sha1, facets = parse.do_burner_work_html(
                            body, f.body_bytes, resp_headers_list, url=url)
                json_log['checksum'] = sha1

                if self.facetlogfd:
                    print(json.dumps({'url': url.url, 'facets': facets}, sort_keys=True), file=self.facetlogfd)

                LOGGER.debug('parsing content of url %r returned %d links, %d embeds, %d facets',
                             url.url, len(links), len(embeds), len(facets))
                json_log['found_links'] = len(links) + len(embeds)
                stats.stats_max('max urls found on a page', len(links) + len(embeds))

                new_links = 0
                for u in links:
                    if self.add_url(priority + 1, u):
                        new_links += 1
                for u in embeds:
                    if self.add_url(priority - 1, u):
                        new_links += 1

                if new_links:
                    json_log['found_new_links'] = new_links
                # XXX plugin for links and new links - post to Kafka, etc
                LOGGER.debug('size of work queue now stands at %r urls', self.q.qsize())
                stats.stats_fixed('queue size', self.q.qsize())
                stats.stats_max('max queue size', self.q.qsize())

        if self.crawllogfd:
            print(json.dumps(json_log, sort_keys=True), file=self.crawllogfd)

    async def work(self):
        '''
        Process queue items until we run out.
        '''
        try:
            while True:
                try:
                    work = self.q.get_nowait()
                except asyncio.queues.QueueEmpty:
                    # this self.q.get() is racy with the test for all workers awaiting.
                    # putting it here (except clause) makes sure the race is rarely run.
                    self.awaiting_work += 1
                    with stats.coroutine_state('awaiting work'):
                        work = await self.q.get()
                    self.awaiting_work -= 1
                await self.fetch_and_process(work)
                self.q.task_done()

                if self.stopping:
                    raise asyncio.CancelledError

                if self.paused:
                    with stats.coroutine_state('paused'):
                        while self.paused:
                            await asyncio.sleep(1)

                if self.remaining_url_budget is not None:
                    self.remaining_url_budget -= 1
                    if self.remaining_url_budget <= 0:
                        raise asyncio.CancelledError

        except asyncio.CancelledError:
            pass

    def save(self, f):
        # XXX make this more self-describing
        pickle.dump('Put the XXX header here', f)  # XXX date, conf file name, conf file checksum
        pickle.dump(self.ridealongmaxid, f)
        pickle.dump(self.ridealong, f)
        pickle.dump(self._seeds, f)
        count = self.q.qsize()
        pickle.dump(count, f)
        for _ in range(0, count):
            entry = self.q.get_nowait()
            pickle.dump(entry, f)

    def load(self, f):
        header = pickle.load(f)  # XXX check that this is a good header... log it
        self.ridealongmaxid = pickle.load(f)
        self.ridealong = pickle.load(f)
        self._seeds = pickle.load(f)
        # XXX load seeds
        self.q = asyncio.PriorityQueue(loop=self.loop)
        count = pickle.load(f)
        for _ in range(0, count):
            entry = pickle.load(f)
            self.q.put_nowait(entry)

    def get_savefilename(self):
        savefile = self.config['Save'].get('Name', 'cocrawler-save-$$')
        savefile = savefile.replace('$$', str(os.getpid()))
        savefile = os.path.expanduser(os.path.expandvars(savefile))
        if os.path.exists(savefile) and not self.config['Save'].get('Overwrite'):
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
            self.next_minute = time.time() + 30
            stats.report()

    def update_cpu_stats(self):
        elapsedc = time.clock()  # should be since process start
        stats.stats_fixed('main thread cpu time', elapsedc)

    def summarize(self):
        '''
        Print a human-readable summary of what's in the queues
        '''
        print('{} items in the crawl queue'.format(self.q.qsize()))
        print('{} items in the ridealong dict'.format(len(self.ridealong)))
        urls_with_tries = 0
        priority_count = defaultdict(int)
        netlocs = defaultdict(int)
        for k, v in self.ridealong.items():
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

    async def crawl(self):
        '''
        Run the crawler until it's out of work
        '''
        self.workers = [asyncio.Task(self.work(), loop=self.loop) for _ in range(self.max_workers)]

        # this is now the 'main' coroutine

        if self.config['Multiprocess'].get('Affinity'):
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
                LOGGER.warning('all workers exited, finishing up.')
                break

            if self.awaiting_work == len(self.workers) and self.q.qsize() == 0:
                # this is a little racy with how awaiting work is set and the queue is read
                # while we're in this join we aren't looking for STOPCRAWLER etc
                LOGGER.warning('all workers appear idle, queue appears empty, executing join')
                await self.q.join()
                break

            self.update_cpu_stats()
            self.minute()

            # XXX clear the DNS cache every few hours; currently the
            # in-memory one is kept for the entire crawler run

        self.cancel_workers()

        if self.stopping or self.config['Save'].get('SaveAtExit'):
            self.summarize()
            self.datalayer.summarize()
            LOGGER.warning('saving datalayer and queues')
            self.save_all()
            LOGGER.warning('saving done')
