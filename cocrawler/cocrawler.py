'''
The actual web crawler
'''

import cgi
import urllib.parse
import json
import time
import os
from functools import partial
import pickle
from collections import defaultdict
from operator import itemgetter
import random
import socket

import asyncio
import logging
import aiohttp
import aiohttp.resolver
import aiohttp.connector

import pluginbase

import stats
import seeds
import datalayer
import robots
import parse
import fetcher
import useragent
import urls
import burner
import dns

LOGGER = logging.getLogger(__name__)

__version__ = '0.01'

# aiohttp.ClientReponse lacks this method, so...
def is_redirect(response):
    return response.status in (300, 301, 302, 303, 307)

class Crawler:
    def __init__(self, loop, config, load=None, no_test=False):
        self.config = config
        self.loop = loop
        self.burner = burner.Burner(config['Crawl']['BurnerThreads'], loop, 'parser')
        self.burner_parseinburnersize = int(self.config['Crawl']['ParseInBurnerSize'])
        self.stopping = 0
        self.no_test = no_test
        self.next_minute = 0

        self.robotname, self.ua = useragent.useragent(config, __version__)

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
        self.conn_kwargs = {'use_dns_cache': True, 'resolver': resolver}
        if local_addr:
            self.conn_kwargs['local_addr'] = local_addr
        self.conn_kwargs['family'] = socket.AF_INET # XXX config option
        conn = aiohttp.connector.TCPConnector(**self.conn_kwargs)
        self.connector = conn
        self.session = aiohttp.ClientSession(loop=loop, connector=conn,
                                             headers={'User-Agent': self.ua})

        self.q = asyncio.PriorityQueue(loop=self.loop)
        self.ridealong = {}
        self.ridealongmaxid = 1 # XXX switch this to using url_canon as the id

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

        if load is not None:
            self.load_all(load)
            LOGGER.info('after loading saved state, work queue is %r urls', self.q.qsize())
        else:
            self._seeds = seeds.expand_seeds(self.config.get('Seeds', {}))
            for s in self._seeds:
                self.add_url(1, s, seed=True)
            LOGGER.info('after adding seeds, work queue is %r urls', self.q.qsize())
            stats.stats_max('initial seeds', self.q.qsize())

        self.plugin_base = pluginbase.PluginBase(package='cocrawler.plugins')
        plugins_path = config.get('Plugins', {}).get('Path', [])
        fix_plugin_path = partial(os.path.join, os.path.abspath(os.path.dirname(__file__)))
        plugins_path = [fix_plugin_path(x) for x in plugins_path]
        self.plugin_source = self.plugin_base.make_plugin_source(searchpath=plugins_path)
        self.plugins = {}
        for plugin_name in self.plugin_source.list_plugins():
            if plugin_name.startswith('test_'):
                continue
            plugin = self.plugin_source.load_plugin(plugin_name)
            plugin.setup(self, config)
        LOGGER.info('Installed plugins: %s', ','.join(sorted(list(self.plugins.keys()))))

        self.max_workers = int(self.config['Crawl']['MaxWorkers'])
        self.remaining_url_budget = self.config['Crawl'].get('MaxCrawledUrls')
        # XXX surely there's a less ugly way to do the following:
        if self.remaining_url_budget is not None:
            self.remaining_url_budget = int(self.remaining_url_budget)
        self.awaiting_work = 0

        LOGGER.info('Touch ~/STOPCRAWLER.%d to stop the crawler.', os.getpid())

    @property
    def seeds(self):
        return self._seeds

    @property
    def qsize(self):
        return self.q.qsize()

    def register_plugin(self, name, plugin_function):
        self.plugins[name] = plugin_function

    def log_rejected_add_url(self, url):
        if self.rejectedaddurlfd:
            print(url, file=self.rejectedaddurlfd)

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
        if not seed and not self.plugins['url_allowed'](url):
            LOGGER.debug('url %r was rejected by url_allow.', url)
            stats.stats_sum('rejected by url_allowed', 1)
            self.log_rejected_add_url(url)
            return
        # end allow/deny plugin

        LOGGER.debug('actually adding url %r', url)
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
        stats.coroutine_report()
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
        work = self.ridealong[ra]
        url = work['url']
        tries = work.get('tries', 0)
        maxtries = self.config['Crawl']['MaxTries']

        parts = urllib.parse.urlparse(url)
        headers, proxy, mock_url, mock_robots = fetcher.apply_url_policies(url, parts, self.config)

        with stats.coroutine_state('fetching/checking robots'):
            r = await self.robots.check(url, parts, headers=headers, proxy=proxy, mock_robots=mock_robots)
        if not r:
            # XXX there are 2 kinds of fail, no robots data and robots denied. robotslog has the full details.
            # XXX treat 'no robots data' as a soft failure?
            # XXX log more particular robots fail reason here
            json_log = {'type':'get', 'url':url, 'priority':priority, 'status':'robots', 'time':time.time()}
            if self.crawllogfd:
                print(json.dumps(json_log, sort_keys=True), file=self.crawllogfd)
            del self.ridealong[ra]
            return

        # XXX response.release asap. btw response.text does one for you
        f = await fetcher.fetch(url, parts, self.session, self.config,
                                headers=headers, proxy=proxy, mock_url=mock_url)

        json_log = {'type':'get', 'url':url, 'priority':priority,
                    't_first_byte':f.t_first_byte, 'time':time.time()}
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
            work['tries'] = tries
            work['priority'] = priority
            self.ridealong[ra] = work
            self.q.put_nowait((priority, rand, ra))
            return

        del self.ridealong[ra]

        json_log['status'] = f.response.status

        if is_redirect(f.response):
            headers = f.response.headers
            location = f.response.headers.get('location')
            location = urls.clean_webpage_links(location)
            next_url = urllib.parse.urljoin(url, location)
            next_url_canon = urls.safe_url_canonicalization(next_url)
            priority += 1

            # XXX make sure it didn't redirect to itself
            #  e.g. only a capitalization change in hostname
            #  e.g. only a capitalization change in the path - could be real, should allow even if seen before
            #  e.g. only adding/removing www. - mark in last-k data structure for pre-processing
            # some hosts redir to exactly themselves while setting cookies, e.g. nyt
            # another case: directories: example.com/dir -> exmaple.com/dir/ and vice versa
            # XXX need surt-surt comparison and seen_url check

            json_log['redirect'] = next_url

            kwargs = {}
            if 'seed' in work:
                if 'seedredirs' in work:
                    work['seedredirs'] += 1
                else:
                    work['seedredirs'] = 1
                if work['seedredirs'] > self.config['Seeds'].get('SeedRedirsCount', 0):
                    del work['seed']
                    del work['seedredirs']
                else:
                    kwargs['seed'] = work['seed']
                    kwargs['seedredirs'] = work['seedredirs']
                    if self.config['SeedRedirsFree']:
                        priority -= 1
                    json_log['seedredirs'] = work['seedredirs']

            if self.add_url(priority+1, next_url, **kwargs): # XXX add more policy regarding priorities
                json_log['found_new_links'] = 1
            # fall through to release and json logging

        # if 200, parse urls out of body
        if f.response.status == 200:
            headers = f.response.headers
            content_type = f.response.headers.get('content-type')
            if content_type:
                content_type, _ = cgi.parse_header(content_type)
            else:
                content_type = 'Unknown'
            LOGGER.debug('url %r came back with content type %r', url, content_type)
            json_log['content_type'] = content_type
            stats.stats_sum('content-type=' + content_type, 1)
            # PLUGIN: post_crawl_200 by content type
            # post_crawl_raw(header_bytes, body_bytes, response.status, time.time())
            # for example, add to a WARC, or post to a Kafka queue
            if content_type == 'text/html':
                try:
                    with stats.record_burn('response.text() decode', url=url):
                        body = await f.response.text() # do not use encoding found in the headers -- policy
                        # XXX consider using 'ascii' for speed, if all we want to do is regex in it
                except (UnicodeDecodeError, LookupError):
                    # LookupError: .text() guessed an encoding that decode() won't understand (wut?)
                    # XXX if encoding was in header, maybe I should use it?
                    # XXX can get additional exceptions here, broken tcp connect etc. see list in fetcher
                    body = f.body_bytes.decode(encoding='utf-8', errors='replace')

                if len(body) > self.burner_parseinburnersize:
                    links, embeds = await self.burner.burn(partial(parse.find_html_links, body, url=url), url=url)
                else:
                    with stats.coroutine_state('await parser'):
                        links, embeds = parse.find_html_links(body, url=url)

                LOGGER.debug('parsing content of url %r returned %d links, %d embeds',
                             url, len(links), len(embeds))
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
                stats.stats_max('max queue size', self.q.qsize())

        await f.response.release()
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
                    # this is racy with the test for all workers awaiting.
                    # putting it here makes sure the race is rarely run.
                    self.awaiting_work += 1
                    work = await self.q.get()
                    self.awaiting_work -= 1
                await self.fetch_and_process(work)
                self.q.task_done()

                if self.stopping:
                    raise asyncio.CancelledError

                if self.remaining_url_budget is not None:
                    self.remaining_url_budget -= 1
                    if self.remaining_url_budget <= 0:
                        raise asyncio.CancelledError

        except asyncio.CancelledError:
            pass

    def save(self, f):
        # XXX make this more self-describing
        pickle.dump('Put the XXX header here', f) # XXX date, conf file name, conf file checksum
        pickle.dump(self.ridealongmaxid, f)
        pickle.dump(self.ridealong, f)
        pickle.dump(self._seeds, f)
        count = self.q.qsize()
        pickle.dump(count, f)
        for _ in range(0, count):
            entry = self.q.get_nowait()
            pickle.dump(entry, f)

    def load(self, f):
        header = pickle.load(f) # XXX check that this is a good header... log it
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
            parts = urllib.parse.urlparse(url)
            netlocs[parts.netloc] += 1
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

        while True:
            await asyncio.sleep(1)

            if os.path.exists(os.path.expanduser('~/STOPCRAWLER.{}'.format(os.getpid()))):
                LOGGER.warning('saw STOPCRAWLER file, stopping crawler and saving queues')
                self.stopping = 1

            self.workers = [w for w in self.workers if not w.done()]
            LOGGER.debug('%d workers remain', len(self.workers))
            if len(self.workers) == 0:
                LOGGER.warning('all workers exited, finishing up.')
                break

            print('checking to see if awaiting {} equals workers {}'.format(self.awaiting_work, len(self.workers)))
            if self.awaiting_work == len(self.workers) and self.q.qsize() == 0:
                # this is a little racy with how awaiting work is set and the queue is read
                # while we're in this join we aren't looking for STOPCRAWLER etc
                LOGGER.warning('all workers appear idle, executing join')
                await self.q.join()
                break

            stats.coroutine_report()
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
