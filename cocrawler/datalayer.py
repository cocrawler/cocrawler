# datalayer, naieve implemenetion.
# all in-process-memory

#database layer spec

# host database
#   counters: urls seen, ...
# url database
#   surt url, last-crawl-date, ranking counters
# per-host crawl frontiers ordered by rank
#   lossy? refresh by iterating over url database
#   python Queue is single-process and not ranked
# robots cache, with timeout
# path to seed - naive or accurate?

#import sortedcontainers - I wish!
import cachetools.ttl
import unittest

class Datalayer:
    def __init__(self, config):
        self.config = config
        self.seen_urls = set()

        robots_size = config.get('Robots', {}).get('RobotsCacheSize')
        robots_ttl = config.get('Robots', {}).get('RobotsCacheTimeout')
        self.robots = cachetools.ttl.TTLCache(robots_size, robots_ttl)

    # This is the minimum url database:
    # as part of a "should we add this url to the queue?" process,
    # we need to remember all queued urls.

    def add_seen_url(self, url):
        self.seen_urls.add(url)

    def seen_url(self, url):
        return url in self.seen_urls

    # collections.TTLCache is built on collections.OrderedDict and not sortedcontainers :-(
    # so it may need replacing if someone wants to do a survey crawl
    # XXX may need to become async so other implemtations can do an outcall?

    def cache_robots(self, schemenetloc, contents):
        self.robots[schemenetloc] = contents

    def read_robots_cache(self, schemenetloc):
        return self.robots[schemenetloc]

class TestUrlAllowed(unittest.TestCase):
    def setUp(self):
        self.datalayer = Datalayer({'Robots':{'RobotsCacheSize':1, 'RobotsCacheTimeout': 1}})

    def test_seen(self):
        self.assertFalse(self.datalayer.seen_url('example.com'))
        self.datalayer.add_seen_url('example.com')
        self.assertTrue(self.datalayer.seen_url('example.com'))

    def test_datalayer(self):
        self.assertRaises(KeyError, self.datalayer.read_robots_cache, 'http://example.com')
        self.datalayer.cache_robots('http://example.com', b'THIS IS A TEST')
        self.assertEqual(self.datalayer.read_robots_cache('http://example.com'), b'THIS IS A TEST')

if __name__ == '__main__':
    unittest.main()
