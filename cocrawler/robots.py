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

import robotexclusionrulesparser
import magic

from .urls import URL
from . import stats
from . import fetcher
from . import config
from . import post_fetch

LOGGER = logging.getLogger(__name__)


def strip_bom(b):
    if b[:3] == b'\xef\xbb\xbf':  # utf-8, e.g. microsoft.com's sitemaps
        return b[3:].lstrip()
    elif b[:2] in (b'\xfe\xff', b'\xff\xfe'):  # utf-16 BE and LE, respectively
        return b[2:].lstrip()
    else:
        return b.lstrip()


def preprocess_robots(text, robotname, json_log):
    '''
    robotsexclusionrulesparser does not follow the de-facto robots.txt standard.
    1) blank lines should not reset user-agent to *
    2) user agent names should not regex or substring
        (e.g. crawler foo-bar should not match a rule for user-agent '-')
    3) longest match
    This code preprocesses robots.txt to mitigate (1), and does clever things to fix (2)

    TODO: Mitigate (3)
    TODO: wrap robotsexclusionrulesparser in another class?

    Note: Python's built-in urllib.robotparser definitely breaks (1)
    '''
    robots = ''
    line_count = 0
    for line in text.splitlines():
        if '#' in line:
            line = line.split('#', 1)[0]
        line = line.strip()
        if len(line):
            line_count += 1
            robots += line + '\n'

    user_agents = re.findall(r'\s* User-Agent: \s* (.*)', robots, re.X | re.I)
    action_lines = len(re.findall(r'(allow|disallow|crawl-delay)', robots, re.X | re.I))

    user_agents = list(set([u.lower() for u in user_agents]))

    mentions_us = robotname.lower() in user_agents
    if mentions_us:
        json_log['mentions-us'] = True

    if user_agents:
        json_log['user-agents'] = len(user_agents)
    if action_lines:
        json_log['action-lines'] = action_lines
    if text:
        json_log['size'] = len(text)

    return robots, mentions_us


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
        self.magic = magic.Magic(flags=magic.MAGIC_MIME_TYPE)
        self.robotslog = config.read('Logging', 'Robotslog')
        if self.robotslog:
            self.robotslogfd = open(self.robotslog, 'a')
        else:
            self.robotslogfd = None

    def __del__(self):
        if self.magic is not None:
            self.magic.close()
        if self.robotslogfd:
            self.robotslogfd.close()

    async def check(self, url, host_geoip, seed_host, crawler, headers=None, proxy=None, mock_robots=None):
        schemenetloc = url.urlsplit.scheme + '://' + url.urlsplit.netloc

        try:
            robots, mentions_us = self.datalayer.read_robots_cache(schemenetloc)
            stats.stats_sum('robots cache hit', 1)
        except KeyError:
            robots, mentions_us = await self.fetch_robots(schemenetloc, mock_robots, host_geoip, seed_host, crawler,
                                                          headers=headers, proxy=proxy)

        if url.urlsplit.path:
            pathplus = url.urlsplit.path
        else:
            pathplus = '/'
        if url.urlsplit.query:
            pathplus += '?' + url.urlsplit.query

        if robots is None:
            LOGGER.debug('no robots info known for %s, failing %s%s', schemenetloc, schemenetloc, pathplus)
            self.jsonlog(schemenetloc, {'error': 'no robots info known', 'action': 'deny'})
            stats.stats_sum('robots denied - robots info not known', 1)
            stats.stats_sum('robots denied', 1)
            return False

        me = self.robotname
        if not mentions_us:
            me = '*'  # works around bug in robotsexclusionparser

        with stats.record_burn('robots is_allowed', url=schemenetloc):
            check = robots.is_allowed(me, pathplus)
            if not check:
                google_check = robots.is_allowed('googlebot', pathplus)
                if me != '*':
                    generic_check = robots.is_allowed('*', pathplus)
                else:
                    generic_check = None

        if check:
            LOGGER.debug('robots allowed for %s%s', schemenetloc, pathplus)
            stats.stats_sum('robots allowed', 1)
            return True

        LOGGER.debug('robots denied for %s%s', schemenetloc, pathplus)
        stats.stats_sum('robots denied', 1)

        json_log = {'url': pathplus, 'action': 'deny'}

        if google_check:
            json_log['google-action'] = 'allow'
            stats.stats_sum('robots denied - but googlebot allowed', 1)
        if generic_check is not None and generic_check:
            json_log['generic-action'] = 'allow'
            stats.stats_sum('robots denied - but * allowed', 1)

        self.jsonlog(schemenetloc, json_log)
        return False

    def _cache_empty_robots(self, schemenetloc, final_schemenetloc):
        parsed = robotexclusionrulesparser.RobotExclusionRulesParser()
        parsed.parse('')
        self.datalayer.cache_robots(schemenetloc, (parsed, False))
        if final_schemenetloc:
            self.datalayer.cache_robots(final_schemenetloc, (parsed, False))
        self.in_progress.discard(schemenetloc)
        return parsed, False

    async def fetch_robots(self, schemenetloc, mock_url, host_geoip, seed_host, crawler, headers=None, proxy=None):
        '''
        robotexclusionrules fetcher is not async, so fetch the file ourselves

        https://developers.google.com/search/reference/robots_txt
        3xx redir == follow up to 5 hops, then consider it a 404.
        4xx errors == no crawl restrictions
        5xx errors == full disallow. fast retry if 503.
           if site appears to return 5xx for 404, then 5xx is treated as a 404
        '''
        url = URL(schemenetloc + '/robots.txt')

        if proxy:
            raise ValueError('not yet implemented')

        # We might enter this routine multiple times, so, sleep if we aren't the first
        if schemenetloc in self.in_progress:
            while schemenetloc in self.in_progress:
                LOGGER.debug('sleeping because someone beat me to the robots punch')
                # XXX make this a stat?
                with stats.coroutine_state('robots collision sleep'):
                    interval = random.uniform(0.2, 0.3)
                    await asyncio.sleep(interval)

            # at this point robots might be in the cache... or not.
            try:
                (robots, mentions_us) = self.datalayer.read_robots_cache(schemenetloc)
            except KeyError:
                robots = None
            if robots is not None:
                return robots, mentions_us

            # ok, so it's not in the cache -- and the other guy's fetch failed.
            # if we just fell through, there would be a big race.
            # treat this as a "no data" failure.
            LOGGER.debug('some other fetch of robots has failed.')  # XXX make this a stat
            return None, False

        self.in_progress.add(schemenetloc)

        f = await fetcher.fetch(url, self.session, max_page_size=self.max_robots_page_size,
                                headers=headers, proxy=proxy, mock_url=mock_url,
                                allow_redirects=True, max_redirects=5, stats_prefix='robots ')

        json_log = {'action': 'fetch', 'time': time.time(), 'host': schemenetloc}

        if f.last_exception:
            json_log['error'] = 'max tries exceeded, final exception is: ' + f.last_exception
            self.jsonlog(schemenetloc, json_log)
            self.in_progress.discard(schemenetloc)
            return None, False

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

        # if the final status is a redirect, we exceeded max redirects -- treat as a 404, same as googlebot
        # Googlebot treats all 4xx as an empty robots.txt
        if str(status).startswith('3') or str(status).startswith('4'):
            if status >= 400:
                error = 'got a 4xx, treating as empty robots'
            else:
                error = 'got too many redirects, treating as empty robots'
            json_log['error'] = error
            self.jsonlog(schemenetloc, json_log)
            return self._cache_empty_robots(schemenetloc, final_schemenetloc)

        # Googlebot treats all 5xx as deny, unless they think the host returns 5xx instead of 404:
        # XXX implement googlebot strategy
        if str(status).startswith('5'):
            json_log['error'] = 'got a 5xx, treating as deny'
            self.jsonlog(schemenetloc, json_log)
            self.in_progress.discard(schemenetloc)
            return None, False

        # we got a 2xx, so let's use the final headers to facet the final server
        if final_schemenetloc:
            robots_url = final_schemenetloc + '/robots.txt'
            # if the hostname is the same and only the scheme is different, that's ok
            if ((robots_url.replace('https://', 'http://', 1) != url.url and
                 robots_url.replace('http://', 'https://', 1) != url.url)):
                host_geoip = {}  # the passed-in one is for the initial server
        else:
            robots_url = url.url
        post_fetch.post_robots_txt(f, robots_url, host_geoip, json_log['time'], crawler, seed_host=seed_host)

        body_bytes = f.body_bytes

        with stats.record_burn('robots sha1'):
            sha1 = 'sha1:' + hashlib.sha1(body_bytes).hexdigest()
        json_log['checksum'] = sha1

        body_bytes = strip_bom(body_bytes)

        plausible, message = is_plausible_robots(body_bytes)
        if not plausible:
            # policy: treat as empty
            json_log['error'] = 'saw an implausible robots.txt, treating as empty'
            json_log['implausible'] = message
            self.jsonlog(schemenetloc, json_log)
            return self._cache_empty_robots(schemenetloc, final_schemenetloc)

        # go from bytes to a string, despite bogus utf8
        # XXX what about non-utf8?
        try:
            body = body_bytes.decode(encoding='utf8', errors='replace')
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # log as surprising, also treat like a fetch error
            json_log['error'] = 'robots body decode threw a surprising exception: ' + repr(e)
            self.jsonlog(schemenetloc, json_log)
            self.in_progress.discard(schemenetloc)
            return None, False

        preprocessed, mentions_us = preprocess_robots(body, self.robotname, json_log)

        with stats.record_burn('robots parse', url=schemenetloc):
            robots = robotexclusionrulesparser.RobotExclusionRulesParser()
            robots.parse(preprocessed)

        with stats.record_burn('robots is_allowed', url=schemenetloc):
            check = robots.is_allowed('*', '/')
            if not check:
                json_log['generic-deny-slash'] = True
                check = robots.is_allowed('googlebot', '/')
                json_log['google-deny-slash'] = not check

        self.datalayer.cache_robots(schemenetloc, (robots, mentions_us))
        self.in_progress.discard(schemenetloc)
        if final_schemenetloc:
            self.datalayer.cache_robots(final_schemenetloc, (robots, mentions_us))
            # we did not set this but we'll discard it anyway
            self.in_progress.discard(final_schemenetloc)
        if robots.sitemaps:
            json_log['has-sitemaps'] = len(robots.sitemaps)

        self.jsonlog(schemenetloc, json_log)
        return robots, mentions_us

    def jsonlog(self, schemenetloc, json_log):
        if self.robotslogfd:
            print(json.dumps(json_log, sort_keys=True), file=self.robotslogfd)
