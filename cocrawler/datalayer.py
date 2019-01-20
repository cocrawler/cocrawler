import pickle
import logging
import cachetools.ttl

from . import config
from . import memory

LOGGER = logging.getLogger(__name__)
__NAME__ = 'datalayer seen memory'


class Datalayer:
    def __init__(self):
        self.seen_set = set()

        robots_size = config.read('Robots', 'RobotsCacheSize')
        robots_ttl = config.read('Robots', 'RobotsCacheTimeout')
        self.robots = cachetools.ttl.TTLCache(robots_size, robots_ttl)

        memory.register_debug(self.memory)

    def add_seen(self, url):
        '''A "seen" url is one that we've done something with, such as having
        queued it or already crawled it.'''
        self.seen_set.add(url.surt)

    def seen(self, url):
        return url.surt in self.seen_set

    def cache_robots(self, schemenetloc, parsed):
        self.robots[schemenetloc] = parsed

    def read_robots_cache(self, schemenetloc):
        return self.robots[schemenetloc]

    def save(self, f):
        pickle.dump(__NAME__, f)
        pickle.dump(self.seen_set, f)
        # don't save robots cache

    def load(self, f):
        name = pickle.load(f)
        if name != __NAME__:
            LOGGER.error('save file name does not match datalayer name: %s != %s', name, __NAME__)
            raise ValueError
        self.seen_set = pickle.load(f)

    def summarize(self):
        '''Print a human-readable sumary of what's in the datalayer'''
        print('{} seen'.format(len(self.seen_set)))

    def memory(self):
        '''Return a dict summarizing the datalayer's memory usage'''
        seen_set = {}
        seen_set['bytes'] = memory.total_size(self.seen_set)
        seen_set['len'] = len(self.seen_set)
        robots = {}
        robots['bytes'] = memory.total_size(self.robots)
        robots['len'] = len(self.robots)
        return {'seen_set': seen_set, 'robots': robots}
