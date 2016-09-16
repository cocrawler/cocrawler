'''
A trivial stats system for CoCrawler
'''

import logging
import pickle
import time
from contextlib import contextmanager

from sortedcontainers import SortedSet

LOGGER = logging.getLogger(__name__)

start_time = time.time()
start_cpu = time.clock()
maxes = {}
sums = {}
burners = {}
coroutine_states = {}
exitstatus = 0

def stats_max(name, value):
    maxes[name] = max(maxes.get(name, value), value)

def stats_sum(name, value):
    sums[name] = sums.get(name, 0) + value

def mynegsplitter(string):
    _, value = string.rsplit(':', maxsplit=1)
    return -float(value)

def record_cpu_burn(name, start, url=None):
    elapsed = time.clock() - start
    burn = burners.get(name, {})
    burn['count'] = burn.get('count', 0) + 1
    burn['time'] = burn.get('time', 0.0) + elapsed

    # are we exceptional? 10x current average
    if elapsed > burn['time']/burn['count'] * 10:
        if 'list' not in burn:
            burn['list'] = SortedSet(key=mynegsplitter)
        url = url or 'none'
        burn['list'].add(url + ':' + str(elapsed))

    burners[name] = burn

@contextmanager
def record_burn(name, url=None):
    try:
        start = time.clock()
        yield
    finally:
        record_cpu_burn(name, start, url=url)

@contextmanager
def coroutine_state(k):
    # the documentation for generators leaves something to be desired
    coroutine_states[k] = coroutine_states.get(k,0) + 1
    LOGGER.debug('coroutine entering state %s', k)
    try:
        yield
    finally:
        LOGGER.debug('coroutine exiting state %s', k)
        coroutine_states[k] -= 1

def coroutine_report():
    LOGGER.info('Coroutine report:')
    for k in sorted(list(coroutine_states.keys())):
        LOGGER.info('  %s: %d', k, coroutine_states[k])

def report():
    LOGGER.info('Stats report:')
    for s in sorted(sums):
        LOGGER.info('  %s: %d', s, sums[s])
    for s in sorted(maxes):
        LOGGER.info('  %s: %d', s, maxes[s])

    LOGGER.info('CPU burn report:')
    for key, burn in sorted(burners.items(), key=lambda x: x[1]['time'], reverse=True):
        LOGGER.info('  %s has %d calls taking %.3f cpu seconds.', key, burn['count'], burn['time'])
        if burn.get('list'):
            LOGGER.info('    biggest burners')
            first10 = burn['list'][0:min(len(burn['list']), 10)]
            for url in first10:
                u, e = url.rsplit(':', maxsplit=1)
                LOGGER.info('      %.3fs: %s', float(e), u)

    LOGGER.info('Summary:')
    elapsed = time.time() - start_time
    elapsedc = time.clock() - start_cpu
    LOGGER.info('  Elapsed time is %.3f seconds', elapsed)
    LOGGER.info('  Elapsed cpu time is %.3f seconds', elapsedc)
    if sums.get('URLs fetched', 0) and elapsed > 0:
        LOGGER.info('  Crawl rate is %d pages/second', int(sums['URLs fetched']/elapsed))
    if sums.get('URLs fetched', 0) and elapsedc > 0:
        LOGGER.info('  Crawl rate is %d pages/cpu-second', int(sums['URLs fetched']/elapsedc))

def stat_value(name):
    if name in sums:
        return sums[name]
    if name in maxes:
        return maxes[name]

def check(config):
    seq = config.get('Testing', {}).get('StatsEQ', {})
    global exitstatus
    if seq:
        for s in seq:
            if stat_value(s) != seq[s]:
                if stat_value(s) is None and seq[s] == 0:
                    continue
                LOGGER.error('Stat %s=%s is not the expected value of %s', s, stat_value(s), seq[s])
                exitstatus = 1
            else:
                LOGGER.debug('Stat %s=%s is the expected value', s, seq[s])

def save(f):
    pickle.dump('stats', f)
    pickle.dump(start_time, f)
    pickle.dump(burners, f)
    pickle.dump(maxes, f)
    pickle.dump(sums, f)

def load(f):
    if pickle.load(f) != 'stats':
        raise ValueError('invalid stats section in savefile')
    global start_time
    start_time = pickle.load(f)
    global burners
    burners = pickle.load(f)
    global maxes
    maxes = pickle.load(f)
    global sums
    sums = pickle.load(f)
