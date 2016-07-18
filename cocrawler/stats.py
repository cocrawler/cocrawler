'''
A trivial stats system for CoCrawler
'''

import logging

import time
from operator import itemgetter
import unittest

LOGGER = logging.getLogger(__name__)

burners = {}
burn_in_progress_name = None
burn_in_progress_start = None
start_time = time.time()
maxes = {}
sums = {}
exitstatus = 0

def stats_max(name, value):
    maxes[name] = max(maxes.get(name, value), value)

def stats_sum(name, value):
    sums[name] = sums.get(name, 0) + value

def begin_cpu_burn(name):
    global burn_in_progress_name
    global burn_in_progress_start
    burn_in_progress_name = name
    burn_in_progress_start = time.clock()

def end_cpu_burn(name):
    if name != burn_in_progress_name:
        raise ValueError('name did not match for begin/end: {} and {}'.format(burn_in_progress_name, name))
    end = time.clock()
    burn = burners.get(name, {})
    burn['count'] = burn.get('count', 0) + 1
    burn['time'] = burn.get('time', 0.0) + end - burn_in_progress_start
    burners[name] = burn

def report():
    LOGGER.info('Stats report:')
    for s in sorted(sums):
        LOGGER.info('  {}: {}'.format(s, sums[s]))
    for s in sorted(maxes):
        LOGGER.info('  {}: {}'.format(s, maxes[s]))

    LOGGER.info('CPU burn report:')
    for key, burn in sorted(burners.items(), key=lambda x: x[1]['time'], reverse=True):
        LOGGER.info('  {} has {} calls taking {:.3f} cpu seconds.'.format(key, burn['count'], burn['time']))

    LOGGER.info('Summary:')
    elapsed = time.time() - start_time
    LOGGER.info('  Elapsed time is {:.3f} seconds'.format(elapsed))
    if sums.get('URLs fetched', 0) and elapsed > 0:
        LOGGER.info('  Crawl rate is {} pages/second'.format(int(sums['URLs fetched']/elapsed)))

def stat_value(name):
    if name in sums:
        return sums[name]
    if name in maxes:
        return maxes[name]
    return None

def test(config):
    seq = config.get('Testing', {}).get('StatsEQ', {})
    global exitstatus
    if seq:
        for s in seq:
            if stat_value(s) != seq[s]:
                LOGGER.error('Stat {}={} is not the expected value of {}'.format(s, stat_value(s), seq[s]))
                exitstatus = 1
            else:
                LOGGER.debug('Stat {}={} is the expected value'.format(s, seq[s]))

class TestUrlAlowed(unittest.TestCase):
    def test_max(self):
        stats_max('foo', 3)
        stats_max('bar', 2)
        stats_max('foo', 5)
        self.assertEqual(maxes['foo'], 5)
        self.assertEqual(maxes['bar'], 2)

    def test_sum(self):
        stats_sum('foo', 3)
        stats_sum('bar', 2)
        stats_sum('foo', 5)
        self.assertEqual(sums['foo'], 8)
        self.assertEqual(sums['bar'], 2)

    def test_burn(self):
        begin_cpu_burn('foo')
        end_cpu_burn('foo')
        self.assertEqual(burners['foo']['count'], 1)
        self.assertTrue(burners['foo']['time'] < 0.01, msg='empty burn is less than 10ms')
        begin_cpu_burn('foo')
        t0 = time.time()
        while time.time() - t0 < 0.1:
            pass
        end_cpu_burn('foo')
        self.assertEqual(burners['foo']['count'], 2)
        self.assertTrue(burners['foo']['time'] > 0.05, msg='100ms burn is more than 50ms cpu')
        self.assertTrue(burners['foo']['time'] < 0.15, msg='100ms burn is less than 150ms cpu')

if __name__ == '__main__':
    unittest.main()
