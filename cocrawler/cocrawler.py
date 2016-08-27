'''
The actual web crawler
'''

import cgi
import urllib.parse
import json
import time
import os
from functools import partial

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

LOGGER = logging.getLogger(__name__)

__version__ = '0.01'

# aiohttp.ClientReponse lacks this method, so...
def is_redirect(response):
    return response.status in (300, 301, 302, 303, 307)

class Crawler:
    def __init__(self, loop, config):
        self.config = config
        self.loop = loop

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
        # XXX if it's a list, make up an array of TCPConnecter objects, and rotate
        # XXX save the kwargs in case we want to make a ProxyConnector deeper down
        conn_kwargs = {'use_dns_cache': True, 'resolver': resolver}
        if local_addr:
            conn_kwargs['local_addr'] = local_addr
        conn = aiohttp.connector.TCPConnector(**conn_kwargs)

        self.connector = conn
        # can use self.session.connector to get the connectcor back ... connector.cached_hosts ...
        self.session = aiohttp.ClientSession(loop=loop, connector=conn,
                                             headers={'User-Agent': self.ua})

        # queue.PriorityQueue has no concept of 'ride along' data. Sigh.
        self.q = asyncio.PriorityQueue(loop=self.loop)
        self.ridealong = {}
        self.ridealongmaxid = 1

        self.datalayer = datalayer.Datalayer(config)
        self.robots = robots.Robots(self.robotname, self.session, self.datalayer, config)
        self.jsonlogfile = config['Logging']['Crawllog']
        if self.jsonlogfile:
            self.jsonlogfd = open(self.jsonlogfile, 'w')

        self._seeds = seeds.expand_seeds(self.config.get('Seeds', {}))
        for s in self._seeds:
            self.add_url(0, s, seed=True)
        LOGGER.info('after adding seeds, work queue is %r urls', self.q.qsize())

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
        self.remaining_url_budget = int(self.config['Crawl'].get('MaxCrawledUrls'))
        self.awaiting_work = 0

    @property
    def seeds(self):
        return self._seeds

    def register_plugin(self, name, plugin_function):
        self.plugins[name] = plugin_function

    def add_url(self, priority, url, seed=False):
        # XXX canonical plugin here?
        url, _ = urllib.parse.urldefrag(url) # drop the frag
        if '://' not in url: # will happen for seeds
            if ':' in url:
                return # things like mailto: ...
            url = 'http://' + url
        # drop meaningless cgi args?
        # uses HSTS to upgrade to https:
        #https://chromium.googlesource.com/chromium/src/net/+/master/http/transport_security_state_static.json
        # use HTTPSEverwhere? would have to have a fallback if https failed

        # XXX optionally generate additional urls plugin here
        # e.g. any amazon url with an AmazonID should add_url() the base product page

        # XXX allow/deny plugin modules go here
        # seen url - could also be "seen recently enough"
        if self.datalayer.seen_url(url):
            stats.stats_sum('rejected by seen_urls', 1)
            return
        if not seed and not self.plugins['url_allowed'](url):
            #LOGGER.debug('url %r was rejected by url_allow.', url)
            stats.stats_sum('rejected by url_allowed', 1)
            return
        # end allow/deny plugin

        LOGGER.debug('actually adding url %r', url)
        stats.stats_sum('added urls', 1)

        work = {'url': url}
        self.ridealong[str(self.ridealongmaxid)] = work
        self.q.put_nowait((priority, str(self.ridealongmaxid)))
        self.ridealongmaxid += 1

        self.datalayer.add_seen_url(url)
        return 1

    def close(self):
        LOGGER.info('on the way out, connector.cached_hosts is %r', self.connector.cached_hosts)
        stats.report()
        stats.check(self.config)
        self.session.close()
        if self.jsonlogfd:
            self.jsonlogfd.close()
        if self.q.qsize():
            LOGGER.error('non-zero exit qsize=%d', self.q.qsize())
            stats.exitstatus = 1

    async def fetch_and_process(self, work):
        '''
        Fetch and process a single url.
        '''
        priority, ra = work
        d = self.ridealong[ra]
        url = d['url']
        tries = d.get('tries', 0)
        maxtries = self.config['Crawl']['MaxTries']

        parts = urllib.parse.urlparse(url)
        headers, proxy, mock_url, mock_robots = fetcher.apply_url_policies(url, parts, self.config)

        if not await self.robots.check(url, parts, headers=headers, proxy=proxy, mock_robots=mock_robots):
            # XXX there are 2 kinds of fail, no robots data and robots denied. robotslog has the full details.
            # XXX treat 'no robots data' as a soft failure?
            # XXX log more particular robots fail reason here
            json_log = {'type':'get', 'url':url, 'priority':priority, 'status':'robots', 'time':time.time()}
            print(json.dumps(json_log, sort_keys=True), file=self.jsonlogfd)
            return

        response, body_bytes, header_bytes, apparent_elapsed, last_exception = await fetcher.fetch(
            url, self.session, self.config, headers=headers, proxy=proxy, mock_url=mock_url
        )

        json_log = {'type':'get', 'url':url, 'priority':priority,
                    'apparent_elapsed':apparent_elapsed, 'time':time.time()}
        if tries:
            json_log['retry'] = tries

        if last_exception is not None:
            tries += 1
            if tries > maxtries:
                # XXX log something about exceeding max tries
                # XXX remember that host had a fail
                return
            d['tries'] = tries
            self.ridealong[ra] = d
            self.q.put_nowait((priority, ra))
            return
        del self.ridealong[ra]

        json_log['status'] = response.status

        # PLUGIN: post_crawl_raw(header_bytes, body_bytes, response.status, time.time())
        # for example, add to a WARC, or post to a Kafka queue

        if is_redirect(response):
            headers = response.headers
            location = response.headers.get('location')
            next_url = urllib.parse.urljoin(url, location)
            # XXX make sure it didn't redirect to itself.
            # XXX some hosts redir to themselves while setting cookies,
            #  that's an infinite loop if we aren't accepting cookies like PHPSESSIONID
            json_log['redirect'] = next_url
            if self.add_url(priority, next_url): # keep same priority? XXX policy
                json_log['found_new_links'] = 1
            # fall through to release and json logging

        # if 200, parse urls out of body
        if response.status == 200:
            headers = response.headers
            content_type = response.headers.get('content-type')
            if content_type:
                content_type, _ = cgi.parse_header(content_type)
            else:
                content_type = 'Unknown'
            LOGGER.debug('url %r came back with content type %r', url, content_type)
            json_log['content_type'] = content_type
            stats.stats_sum('content-type=' + content_type, 1)
            # PLUGIN: post_crawl_200 by content type
            if content_type == 'text/html':
                try:
                    start = time.clock()
                    body = await response.text() # do not use encoding found in the headers -- policy
                    # XXX consider using 'ascii' for speed, if all we want to do is regex in it
                    stats.record_cpu_burn('response.text() decode', start)
                except UnicodeDecodeError:
                    # XXX if encoding was in header, maybe I should use it?
                    body = body_bytes.decode(encoding='utf-8', errors='replace')

                # PLUGIN post_crawl_200_find_urls -- links and/or embeds
                # should have an option to run this in a separate process or fork,
                #  so as to not cpu burn in the main process
                urls = parse.find_html_links(body)
                LOGGER.debug('parsing content of url %r returned %r links', url, len(urls))
                json_log['found_links'] = len(urls)
                stats.stats_max('max urls found on a page', len(urls))

                new_links = 0
                for u in urls:
                    new_url = urllib.parse.urljoin(url, u)
                    if self.add_url(priority + 1, new_url): # XXX if embed, priority - 1
                        new_links += 1
                if new_links:
                    json_log['found_new_links'] = new_links
                # XXX plugin for links and new links - post to Kafka, etc
                LOGGER.debug('size of work queue now stands at %r urls', self.q.qsize())
                stats.stats_max('max queue size', self.q.qsize())

        await response.release() # No pipelining
        print(json.dumps(json_log, sort_keys=True), file=self.jsonlogfd)

    async def work(self):
        '''
        Process queue items until we run out.
        '''
        try:
            while True:
                self.awaiting_work += 1
                work = await self.q.get()
                self.awaiting_work -= 1
                await self.fetch_and_process(work)
                self.q.task_done()

                if self.remaining_url_budget is not None:
                    self.remaining_url_budget -= 1
                    if self.remaining_url_budget <= 0:
                        raise asyncio.CancelledError

        except asyncio.CancelledError:
            pass

    async def crawl(self):
        '''
        Run the crawler until it's out of work
        '''
        workers = [asyncio.Task(self.work(), loop=self.loop) for _ in range(self.max_workers)]

        if self.remaining_url_budget is not None:
            LOGGER.info('lead coroutine waiting until no more workers (url budget seen)')
            while True:
                await asyncio.sleep(1)
                workers = [w for w in workers if not w.done()]
                LOGGER.debug('%d workers remain', len(workers))
                if len(workers) == 0:
                    LOGGER.warning('all workers exited, finishing up.')
                    break
                if self.awaiting_work == len(workers):
                    LOGGER.warning('all workers are awaiting work, finishing up.')
                    break
        else:
            LOGGER.info('lead coroutine waiting for queue to empty')
            await self.q.join()

        for w in workers:
            w.cancel()
