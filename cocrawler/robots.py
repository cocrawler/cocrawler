'''
Stuff related to robots.txt processing
'''

import asyncio

import time
import random
import json
import logging
import urllib.parse
import hashlib
import re

import reppy.robots
#import magic

from .urls import URL
from . import stats
from . import fetcher
from . import config
from . import post_fetch
from . import content

LOGGER = logging.getLogger(__name__)


def strip_bom(b):
    if b[:3] == b'\xef\xbb\xbf':  # utf-8, e.g. microsoft.com's sitemaps
        return b[3:]
    elif b[:2] in (b'\xfe\xff', b'\xff\xfe'):  # utf-16 BE and LE, respectively
        return b[2:]
    else:
        return b


def robots_facets(text, robotname, json_log):
    user_agents = re.findall(r'^ \s* User-Agent: \s* (.*) \s* (?:\#.*)?', text, re.X | re.I | re.M)
    action_lines = len(re.findall(r'^ \s* (allow|disallow|crawl-delay):', text, re.X | re.I | re.M))

    user_agents = list(set([u.lower() for u in user_agents]))

    mentions_us = robotname.lower() in user_agents

    if mentions_us:
        json_log['mentions_us'] = True
    if user_agents:
        json_log['user_agents'] = len(user_agents)
    if action_lines:
        json_log['action_lines'] = action_lines
    if text:
        json_log['size'] = len(text)


def is_plausible_robots(body_bytes):
    '''
    Did you know that some sites have a robots.txt that's a 100 megabyte video file?

    file magic mimetype is 'text' or similar -- too expensive, 3ms per call
    '''
    if body_bytes.startswith(b'<'):
        return False, 'robots appears to be html or xml'
    if len(body_bytes) > 1000000:
        return False, 'robots is too big'

    return True, ''


class Robots:
    def __init__(self, robotname, session, datalayer):
        self.robotname = robotname
        self.session = session
        self.datalayer = datalayer
        self.max_tries = config.read('Robots', 'MaxTries')
        self.max_robots_page_size = int(config.read('Robots', 'MaxRobotsPageSize'))
        self.in_progress = set()
        # magic is 3 milliseconds per call, too expensive to use
        #self.magic = magic.Magic(flags=magic.MAGIC_MIME_TYPE)
        self.robotslog = config.read('Logging', 'Robotslog')
        if self.robotslog:
            self.robotslogfd = open(self.robotslog, 'a')
        else:
            self.robotslogfd = None

    def __del__(self):
        #if self.magic is not None:
        #    self.magic.close()
        if self.robotslogfd:
            self.robotslogfd.close()

    def check_cached(self, url, quiet=False):
        schemenetloc = url.urlsplit.scheme + '://' + url.urlsplit.netloc

        try:
            robots = self.datalayer.read_robots_cache(schemenetloc)
            stats.stats_sum('robots cached_only hit', 1)
        except KeyError:
            stats.stats_sum('robots cached_only miss', 1)
            return True
        return self._check(url, schemenetloc, robots, quiet=quiet)

    async def check(self, url, dns_entry=None, seed_host=None, crawler=None,
                    get_kwargs={}):
        schemenetloc = url.urlsplit.scheme + '://' + url.urlsplit.netloc

        try:
            robots = self.datalayer.read_robots_cache(schemenetloc)
            stats.stats_sum('robots cache hit', 1)
        except KeyError:
            robots = await self.fetch_robots(schemenetloc, dns_entry, crawler,
                                             seed_host=seed_host, get_kwargs=get_kwargs)
        return self._check(url, schemenetloc, robots)

    def _check(self, url, schemenetloc, robots, quiet=False):
        if url.urlsplit.path:
            pathplus = url.urlsplit.path
        else:
            pathplus = '/'
        if url.urlsplit.query:
            pathplus += '?' + url.urlsplit.query

        if robots is None:
            if quiet:
                return 'no robots'

            LOGGER.debug('no robots info known for %s, failing %s%s', schemenetloc, schemenetloc, pathplus)
            self.jsonlog(schemenetloc, {'error': 'no robots info known', 'action': 'denied'})
            stats.stats_sum('robots denied - robots info not known', 1)
            stats.stats_sum('robots denied', 1)
            return 'no robots'

        me = self.robotname

        with stats.record_burn('robots is_allowed', url=schemenetloc):
            if pathplus.startswith('//') and ':' in pathplus:
                pathplus = 'htp://' + pathplus
            check = robots.allowed(pathplus, me)
            if check:
                check = 'allowed'
            else:
                check = 'denied'
                google_check = robots.allowed(pathplus, 'googlebot')
                if me != '*':
                    generic_check = robots.allowed(pathplus, '*')
                else:
                    generic_check = None

        if quiet:
            return check

        # just logging from here on down

        if check == 'allowed':
            LOGGER.debug('robots allowed for %s%s', schemenetloc, pathplus)
            stats.stats_sum('robots allowed', 1)
            return check

        LOGGER.debug('robots denied for %s%s', schemenetloc, pathplus)
        stats.stats_sum('robots denied', 1)

        json_log = {'url': pathplus, 'action': 'denied'}

        if google_check:
            json_log['google_action'] = 'allowed'
            stats.stats_sum('robots denied - but googlebot allowed', 1)
        if generic_check is not None and generic_check:
            json_log['generic_action'] = 'allowed'
            stats.stats_sum('robots denied - but * allowed', 1)

        self.jsonlog(schemenetloc, json_log)
        return check

    def _cache_empty_robots(self, schemenetloc, final_schemenetloc):
        parsed = reppy.robots.Robots.parse('', '')
        self.datalayer.cache_robots(schemenetloc, parsed)
        if final_schemenetloc:
            self.datalayer.cache_robots(final_schemenetloc, parsed)
        self.in_progress.discard(schemenetloc)
        return parsed

    async def fetch_robots(self, schemenetloc, dns_entry, crawler,
                           seed_host=None, get_kwargs={}):
        '''
        https://developers.google.com/search/reference/robots_txt
        3xx redir == follow up to 5 hops, then consider it a 404.
        4xx errors == no crawl restrictions
        5xx errors == full disallow. fast retry if 503.
           if site appears to return 5xx for 404, then 5xx is treated as a 404
        '''
        url = URL(schemenetloc + '/robots.txt')

        # We might enter this routine multiple times, so, sleep if we aren't the first
        if schemenetloc in self.in_progress:
            while schemenetloc in self.in_progress:
                LOGGER.debug('sleeping because someone beat me to the robots punch')
                stats.stats_sum('robots sleep for collision', 1)
                with stats.coroutine_state('robots collision sleep'):
                    interval = random.uniform(0.2, 0.3)
                    await asyncio.sleep(interval)

            # at this point robots might be in the cache... or not.
            try:
                robots = self.datalayer.read_robots_cache(schemenetloc)
            except KeyError:
                robots = None
            if robots is not None:
                return robots

            # ok, so it's not in the cache -- and the other guy's fetch failed.
            # if we just fell through, there would be a big race.
            # treat this as a "no data" failure.
            LOGGER.debug('some other fetch of robots has failed.')
            stats.stats_sum('robots sleep then cache miss', 1)
            return None

        self.in_progress.add(schemenetloc)

        f = await fetcher.fetch(url, self.session, max_page_size=self.max_robots_page_size,
                                allow_redirects=True, max_redirects=5, stats_prefix='robots ',
                                get_kwargs=get_kwargs)

        json_log = {'action': 'fetch', 'time': time.time()}
        if f.ip is not None:
            json_log['ip'] = f.ip

        if f.last_exception:
            if f.last_exception.startswith('ClientError: TooManyRedirects'):
                error = 'got too many redirects, treating as empty robots'
                json_log['error'] = error
                self.jsonlog(schemenetloc, json_log)
                return self._cache_empty_robots(schemenetloc, None)
            else:
                json_log['error'] = 'max tries exceeded, final exception is: ' + f.last_exception
                self.jsonlog(schemenetloc, json_log)
                self.in_progress.discard(schemenetloc)
                return None

        if f.response.history:
            redir_history = [str(h.url) for h in f.response.history]
            redir_history.append(str(f.response.url))
            json_log['redir_history'] = redir_history

        stats.stats_sum('robots fetched', 1)

        # If the url was redirected to a different host/robots.txt, let's cache that final host too
        final_url = str(f.response.url)  # YARL object
        final_schemenetloc = None
        if final_url != url.url:
            final_parts = urllib.parse.urlsplit(final_url)
            if final_parts.path == '/robots.txt':
                final_schemenetloc = final_parts.scheme + '://' + final_parts.netloc
                json_log['final_host'] = final_schemenetloc

        status = f.response.status
        json_log['status'] = status
        json_log['t_first_byte'] = f.t_first_byte

        if str(status).startswith('3') or str(status).startswith('4'):
            if status >= 400:
                error = 'got a 4xx, treating as empty robots'
            else:
                error = 'too many redirects, treating as empty robots'
            json_log['error'] = error
            self.jsonlog(schemenetloc, json_log)
            return self._cache_empty_robots(schemenetloc, final_schemenetloc)

        if str(status).startswith('5'):
            json_log['error'] = 'got a 5xx, treating as deny'  # same as google
            self.jsonlog(schemenetloc, json_log)
            self.in_progress.discard(schemenetloc)
            return None

        # we got a 2xx, so let's use the final headers to facet the final server
        if dns_entry:
            host_geoip = dns_entry[3]
        else:
            host_geoip = {}
        if final_schemenetloc:
            # if the hostname is the same and only the scheme is different, that's ok
            # TODO: use URL.hostname
            if ((final_url.replace('https://', 'http://', 1) != url.url and
                 final_url.replace('http://', 'https://', 1) != url.url)):
                host_geoip = {}  # the passed-in one is for the initial server
        post_fetch.post_robots_txt(f, final_url, host_geoip, json_log['time'], crawler, seed_host=seed_host)

        body_bytes = f.body_bytes
        content_encoding = f.response.headers.get('content-encoding', 'identity')
        if content_encoding != 'identity':
            body_bytes = content.decompress(f.body_bytes, content_encoding, url=final_url)

        with stats.record_burn('robots sha1'):
            sha1 = 'sha1:' + hashlib.sha1(body_bytes).hexdigest()
        json_log['checksum'] = sha1

        body_bytes = strip_bom(body_bytes).lstrip()

        plausible, message = is_plausible_robots(body_bytes)
        if not plausible:
            # policy: treat as empty
            json_log['error'] = 'saw an implausible robots.txt, treating as empty'
            json_log['implausible'] = message
            self.jsonlog(schemenetloc, json_log)
            return self._cache_empty_robots(schemenetloc, final_schemenetloc)

        try:
            body = body_bytes.decode(encoding='utf8', errors='replace')
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # log as surprising, also treat like a fetch error
            json_log['error'] = 'robots body decode threw a surprising exception: ' + repr(e)
            self.jsonlog(schemenetloc, json_log)
            self.in_progress.discard(schemenetloc)
            return None

        robots_facets(body, self.robotname, json_log)

        with stats.record_burn('robots parse', url=schemenetloc):
            robots = reppy.robots.Robots.parse('', body)

        with stats.record_burn('robots is_allowed', url=schemenetloc):
            check = robots.allowed('/', '*')
            if check == 'denied':
                json_log['generic_deny_slash'] = True
                check = robots.allowed('/', 'googlebot')
                json_log['google_deny_slash'] = check == 'denied'

        self.datalayer.cache_robots(schemenetloc, robots)
        self.in_progress.discard(schemenetloc)
        if final_schemenetloc:
            self.datalayer.cache_robots(final_schemenetloc, robots)
            # we did not set this but we'll discard it anyway
            self.in_progress.discard(final_schemenetloc)
        sitemaps = list(robots.sitemaps)
        if sitemaps:
            json_log['sitemap_lines'] = len(sitemaps)

        self.jsonlog(schemenetloc, json_log)
        return robots

    def jsonlog(self, schemenetloc, json_log):
        if self.robotslogfd:
            json_log['host'] = schemenetloc
            print(json.dumps(json_log, sort_keys=True), file=self.robotslogfd)
