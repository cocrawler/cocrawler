# datalayer, naieve implemenetion.
# all in-process-memory

# database layer spec

# domain database
# host database
#   counters: urls crawled, urls in queue, seen urls, hostrank info like unique incoming C's
#   data: landing pages and their anchortext? or in the url db
#   politeness: current value, last 3 maxes, last N outcomes
#    outcome: 5xx, 4xx, 200, slow-200
#    remember the averages for last 10,100,1000,10k,100k,1mm fetches
# url database
#   seen urls can be a bloom filter
#    one of the ones in pypi does % error, 10 billion @ 0.1% was 17 gigabytes
#   surt url, last-crawl-date, ranking counters
#    can minimize size by only recording details for externally linked urls
# per-host crawl frontiers ordered by rank?
#   lossy? refresh by iterating over url database
#   python Queue is single-process and not ranked
# robots cache, with timeout
# path to seed - naive or accurate?

import pickle
import logging
# import sortedcontainers - I wish! not sure if cachetools.ttl is as efficient
import cachetools.ttl

from . import config

LOGGER = logging.getLogger(__name__)
__NAME__ = 'datalayer seen_urls memory'


class Datalayer:
    def __init__(self):
        self.seen_urls = set()

        robots_size = config.read('Robots', 'RobotsCacheSize')
        robots_ttl = config.read('Robots', 'RobotsCacheTimeout')
        self.robots = cachetools.ttl.TTLCache(robots_size, robots_ttl)

    # This is the minimum url database:
    # as part of a "should we add this url to the queue?" process,
    # we need to remember all queued urls.

    def add_seen_url(self, url):
        self.seen_urls.add(url.surt)

    def seen_url(self, url):
        # do this with a honking bloom filter?
        # notice when an url without cgi args is popular, maybe probe to
        # see if we can guess tracking args vs real ones.
        return url.surt in self.seen_urls

    # collections.TTLCache is built on collections.OrderedDict and not sortedcontainers :-(
    # so it may need replacing if someone wants to do a survey crawl
    # XXX may need to become async so other implemtations can do an outcall?

    def cache_robots(self, schemenetloc, parsed):
        self.robots[schemenetloc] = parsed

    def read_robots_cache(self, schemenetloc):
        return self.robots[schemenetloc]

    def save(self, f):
        pickle.dump(__NAME__, f)
        pickle.dump(self.seen_urls, f)
        # don't save robots cache

    def load(self, f):
        name = pickle.load(f)
        if name != __NAME__:
            LOGGER.error('save file name does not match datalayer name: %s != %s', name, __NAME__)
            raise ValueError
        self.seen_urls = pickle.load(f)

    def summarize(self):
        '''
        print a human-readable sumary of what's in the datalayer
        '''
        print('{} seen_urls'.format(len(self.seen_urls)))
