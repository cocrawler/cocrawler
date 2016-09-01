'''
Stuff related to robots.txt processing
'''

import asyncio

import time
import json
import logging

import robotexclusionrulesparser
import magic

import stats
import fetcher

LOGGER = logging.getLogger(__name__)

class Robots:
    def __init__(self, robotname, session, datalayer, config):
        self.robotname = robotname
        self.session = session
        self.datalayer = datalayer
        self.config = config
        self.rerp = robotexclusionrulesparser.RobotExclusionRulesParser()
        self.max_tries = self.config.get('Robots', {}).get('MaxTries')
        self.in_progress = set()
        self.magic = magic.Magic(flags=magic.MAGIC_MIME_TYPE)
        self.jsonlogfile = self.config.get('Logging', {}).get('Robotslog')
        if self.jsonlogfile:
            self.jsonlogfd = open(self.jsonlogfile, 'w')
        else:
            self.jsonlogfd = None

    async def check(self, url, parts, headers=None, proxy=None, mock_robots=None):
        schemenetloc = parts.scheme + '://' + parts.netloc
        if parts.path:
            pathplus = parts.path
        else:
            pathplus = '/'
        if parts.params:
            pathplus += ';' + parts.params
        if parts.query:
            pathplus += '?' + parts.query

        try:
            robots = self.datalayer.read_robots_cache(schemenetloc)
        except KeyError:
            # extra semantics and policy inside fetch_robots: 404 returns '', etc. also inserts into cache.
            robots = await self.fetch_robots(schemenetloc, mock_robots, headers=headers, proxy=proxy)

        if robots is None:
            self.jsonlog(schemenetloc, {'error':'unable to find robots information', 'action':'deny'})
            stats.stats_sum('robots denied', 1)
            return False

        if len(robots) == 0:
            return True

        start = time.clock()
        self.rerp.parse(robots) # cache this parse?
        stats.record_cpu_burn('robots parse', start)

#        if self.rerp.sitemaps:
#           ...

        start = time.clock()
        check = self.rerp.is_allowed(self.robotname, pathplus)
        stats.record_cpu_burn('robots is_allowed', start)

        if check:
            # don't log success
            return True

        self.jsonlog(schemenetloc, {'url':pathplus, 'action':'deny'})
        stats.stats_sum('robots denied', 1)
        return False

    async def fetch_robots(self, schemenetloc, mock_url, headers=None, proxy=None):
        '''
        robotexclusionrules parser is not async, so fetch the file ourselves
        '''
        url = schemenetloc + '/robots.txt'

        if proxy:
            raise ValueError('not yet implemented')

        # We might enter this routine multiple times, so, sleep if we aren't the first
        # XXX this is frequently racy, according to the logfiles!
        if schemenetloc in self.in_progress:
            while schemenetloc in self.in_progress:
                # XXX make this a stat?
                # XXX does it go off for wide when it shouldn't?
                LOGGER.debug('sleeping because someone beat me to the robots punch')
                await asyncio.sleep(0.3)

            # at this point robots might be in the cache... or not.
            try:
                robots = self.datalayer.read_robots_cache(schemenetloc)
            except KeyError:
                robots = None
            if robots is not None:
                return robots

            # ok, so it's not in the cache -- and the other guy's
            # fetch failed. if we just fell through there would be a
            # big race. treat this as a failure.
            # XXX note that we have no negative caching
            LOGGER.debug('some other fetch of robots has failed.') # XXX make this a stat
            return None

        self.in_progress.add(schemenetloc)

        response, body_bytes, header_bytes, apparent_elapsed, last_exception = await fetcher.fetch(
            url, self.session, self.config, headers=headers, proxy=proxy,
            mock_url=mock_url, allow_redirects=True
        )

        if last_exception:
            self.jsonlog(schemenetloc, {'error':'max tries exceeded, final exception is: ' + last_exception,
                                        'action':'fetch'})
            self.in_progress.discard(schemenetloc)
            return None

        # if we got a 404, return an empty robots.txt
        if response.status == 404:
            self.jsonlog(schemenetloc, {'error':'got a 404, treating as empty robots',
                                        'action':'fetch', 'apparent_elapsed':apparent_elapsed})
            self.datalayer.cache_robots(schemenetloc, '')
            self.in_progress.discard(schemenetloc)
            await response.release()
            return ''

        # if we got a non-200, some should be empty and some should be None (XXX Policy)
        if str(response.status).startswith('4') or str(response.status).startswith('5'):
            self.jsonlog(schemenetloc,
                         {'error':'got an unexpected status of {}, treating as deny'.format(response.status),
                          'action':'fetch', 'apparent_elapsed':apparent_elapsed})
            self.in_progress.discard(schemenetloc)
            await response.release()
            return None

        if not self.is_plausible_robots(schemenetloc, body_bytes, apparent_elapsed):
            # policy: treat as empty
            self.jsonlog(schemenetloc,
                         {'warning':'saw an implausible robots.txt, treating as empty',
                          'action':'fetch', 'apparent_elapsed':apparent_elapsed})
            self.datalayer.cache_robots(schemenetloc, '')
            self.in_progress.discard(schemenetloc)
            await response.release()
            return ''

        # one last thing... go from bytes to a string, despite bogus utf8
        try:
            body = await response.text()
        except UnicodeError: # pragma: no cover
            # something went wrong. try again assuming utf8 and ignoring errors
            body = str(body_bytes, 'utf-8', 'ignore')
        except Exception as e: # pragma: no cover
            # something unusual went wrong. treat like a fetch error.
            self.jsonlog(schemenetloc, {'error':'robots body decode threw an exception: ' + repr(e),
                                        'action':'fetch', 'apparent_elapsed':apparent_elapsed})
            self.in_progress.discard(schemenetloc)
            await response.release()
            return None

        await response.release()
        self.datalayer.cache_robots(schemenetloc, body)
        self.in_progress.discard(schemenetloc)
        self.jsonlog(schemenetloc, {'action':'fetch', 'apparent_elapsed':apparent_elapsed})
        return body

    def is_plausible_robots(self, schemenetloc, body_bytes, apparent_elapsed):
        '''
        Did you know that some sites have a robots.txt that's a 100 megabyte video file?
        '''
        # Not OK: html or xml or something else bad
        if body_bytes.startswith(b'<'): # pragma: no cover
            self.jsonlog(schemenetloc, {'error':'robots appears to be html or xml, ignoring',
                                        'action':'fetch', 'apparent_elapsed':apparent_elapsed})
            return False

        # OK: BOM, it signals a text file ... utf8 or utf16 be/le
        # (this info doesn't appear to be recognized by libmagic?!)
        if (body_bytes.startswith(b'\xef\xbb\xbf') or
                body_bytes.startswith(b'\xfe\xff') or
                body_bytes.startswith(b'\xff\xfe')): # pragma: no cover
            return True

        # OK: file magic mimetype is 'text' or similar
        mime_type = self.magic.id_buffer(body_bytes)
        if not (mime_type.startswith('text') or mime_type == 'application/x-empty') :
            self.jsonlog(schemenetloc, {'error':
                                        'robots has unexpected mimetype {}, ignoring'.format(mime_type),
                                        'action':'fetch', 'apparent_elapsed':apparent_elapsed})
            return False

        # not OK: too big
        if len(body_bytes) > 1000000: # pragma: no cover
            self.jsonlog(schemenetloc, {'error':'robots is too big, ignoring',
                                        'action':'fetch', 'apparent_elapsed':apparent_elapsed})
            return False

        return True

    def jsonlog(self, schemenetloc, d):
        if self.jsonlogfd:
            json_log = d
            json_log['host'] = schemenetloc
            json_log['time'] = '{:.3f}'.format(time.time())
            print(json.dumps(json_log, sort_keys=True), file=self.jsonlogfd, flush=True)
