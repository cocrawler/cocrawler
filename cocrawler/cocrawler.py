'''
The actual web crawler
'''

import cgi
import urllib.parse
import math
import json
import time
import os
from functools import partial

import asyncio
import logging
import aiohttp

import pluginbase

import stats
import seeds
import datalayer
import robots
import parse

LOGGER = logging.getLogger(__name__)

# aiohttp.ClientReponse lacks this method, so...
def is_redirect(response):
    return response.status in (300, 301, 302, 303, 307)

class Crawler:
    def __init__(self, loop, config):
        self.config = config
        self.loop = loop
        useragent = config['Crawl']['UserAgent'] # die if not set
        self.session = aiohttp.ClientSession(loop=loop, headers={'User-Agent': useragent})
        self.q = asyncio.Queue(loop=self.loop)
        self.datalayer = datalayer.Datalayer(config)
        self.robots = robots.Robots(self.session, self.datalayer, config)
        self.jsonlogfile = config['Logging']['Crawllog']
        if self.jsonlogfile:
            self.jsonlogfd = open(self.jsonlogfile, 'w')

        self._seeds = seeds.expand_seeds(self.config.get('Seeds', {}))
        for s in self._seeds:
            self.add_url(s, seed=True)
        LOGGER.info('after adding seeds, work queue is %r urls', self.q.qsize())

        self.plugin_base = pluginbase.PluginBase(package='cocrawler.plugins')
        plugins_path = config.get('Plugins', {}).get('Path', [])
        fix_plugin_path = partial(os.path.join, os.path.abspath(os.path.dirname(__file__)))
        plugins_path = [fix_plugin_path(x) for x in plugins_path]
        self.plugin_source = self.plugin_base.make_plugin_source(searchpath=plugins_path)
        self.plugins = {}
        for plugin_name in self.plugin_source.list_plugins():
            plugin = self.plugin_source.load_plugin(plugin_name)
            plugin.setup(self, config)
        LOGGER.info('Installed plugins: %s', ','.join(sorted(list(self.plugins.keys()))))

        self.max_workers = int(self.config['Crawl']['MaxWorkers'])
        mcu = int(self.config['Crawl'].get('MaxCrawledUrls', 0))
        self.max_crawled_urls = math.ceil(mcu / self.max_workers)

        # testing setup
        if self.config.get('Testing', {}).get('TestHostmapAll'):
            self.test_hostmap_all = self.config['Testing']['TestHostmapAll']
        else:
            self.test_hostmap_all = None

    @property
    def seeds(self):
        return self._seeds

    def register_plugin(self, name, plugin_function):
        self.plugins[name] = plugin_function

    def add_url(self, url, seed=False):
        # XXX canonical plugin here
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
        self.q.put_nowait(url)
        self.datalayer.add_seen_url(url)
        return 1

    def close(self):
        stats.report()
        stats.test(self.config)
        self.session.close()
        if self.jsonlogfd:
            self.jsonlogfd.close()

    # XXX should be a plugin -- does pluginbase deal with async def ?!
    async def fetch_and_process(self, url):
        '''
        Fetch and process a single url.
        '''
        if '://' not in url: # XXX should I deal with this at a higher level?
            url = 'http://' + url

        original_url = url

        headers = {}
        actual_robots = None
        if self.test_hostmap_all:
            parts = urllib.parse.urlparse(url)
            old_netloc = parts.netloc
            # no good way to parse the netloc. it has username, password, host, port
            # XXX just parse host:port
            if ':' in old_netloc:
                old_host, _ = old_netloc.split(':', maxsplit=1)
            else:
                old_host = old_netloc
            headers['Host'] = old_host
            url = parts._replace(netloc=self.test_hostmap_all).geturl()
            actual_robots = parts.scheme + '://' + self.test_hostmap_all + '/robots.txt'

        if not await self.robots.check(original_url, actual_robots=actual_robots, headers=headers):
            # XXX there are 2 kinds of fail, no robots data and robots denied. robotslog has the full details.
            json_log = {'type':'get', 'url':original_url, 'status':'robots', 'time':time.time()}
            print(json.dumps(json_log, sort_keys=True), file=self.jsonlogfd)
            return

        t0 = time.time()

        try:
            response = await self.session.get(url, allow_redirects=False, headers=headers)
            # XXX special sleepy 503 handling here
            # XXX retry handling loop here
        except aiohttp.errors.ClientError as e:
            stats.stats_sum('URL fetch ClientError exceptions', 1)
            # XXX json log something at total fail
            LOGGER.debug('fetching url %r raised %r', url, e)
            raise
        except aiohttp.errors.ServerDisconnectedError as e:
            stats.stats_sum('URL fetch ServerDisconnectedError exceptions', 1)
            # XXX json log something at total fail
            LOGGER.debug('fetching url %r raised %r', url, e)
            raise
        except Exception as e:
            stats.stats_sum('URL fetch Exception exceptions', 1)
            # XXX json log something at total fail
            LOGGER.debug('fetching url %r raised %r', url, e)
            raise

        # fully receive headers and body
        body_bytes = await response.read()
        header_bytes = response.raw_headers

        stats.stats_sum('URLs fetched', 1)
        LOGGER.debug('url %r came back with status %r', url, response.status)
        stats.stats_sum('fetch http code=' + str(response.status), 1)

        # PLUGIN: post_crawl_raw(header_bytes, body_bytes, response.status, time.time())
        # for example, add to a WARC, or post to a Kafka queue
        apparent_elapsed = '{:.3f}'.format(time.time() - t0)
        json_log = {'type':'get', 'url':original_url, 'status':response.status,
                    'apparent_elapsed':apparent_elapsed, 'time':time.time()}

        if is_redirect(response):
            headers = response.headers
            location = response.headers.get('location')
            next_url = urllib.parse.urljoin(original_url, location)
            # XXX make sure it didn't redirect to itself.
            # XXX some hosts redir to themselves while setting cookies
            json_log['redirect'] = next_url
            if self.add_url(next_url):
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
                    body = await response.text() # do not use encoding found in the headers -- policy
                except UnicodeDecodeError as e:
                    # XXX if encoding in header, maybe I should use it?
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
                    url = urllib.parse.urljoin(original_url, u)
                    if self.add_url(url):
                        new_links += 1
                if new_links:
                    json_log['found_new_links'] = new_links
                LOGGER.debug('size of work queue now stands at %r urls', self.q.qsize())
                stats.stats_max('max queue size', self.q.qsize())

        await response.release() # No pipelining
        print(json.dumps(json_log, sort_keys=True), file=self.jsonlogfd)

    async def work(self):
        '''
        Process queue items until we run out.
        '''
        crawled_urls = 0

        try:
            while True:
                url = await self.q.get()
                await self.fetch_and_process(url)
                self.q.task_done()

                # XXX this needs to become a dynamic schedule instead of static
                crawled_urls += 1
                if self.max_crawled_urls and crawled_urls >= self.max_crawled_urls:
                    raise asyncio.CancelledError

        except asyncio.CancelledError:
            pass

    async def crawl(self):
        '''
        Run the crawler until it's out of work
        '''
        workers = [asyncio.Task(self.work(), loop=self.loop) for _ in range(self.max_workers)]

        while True:
            await asyncio.sleep(1)
            # there's no way to do (empty or no workers), so we cheat
            if self.q._unfinished_tasks == 0:
                LOGGER.warning('no tasks remain undone, finishing up.')
                break
            workers = [w for w in workers if not w.done()]
            if len(workers) == 0:
                LOGGER.warning('all workers exited, finishing up.')
                break
        for w in workers:
            w.cancel()
