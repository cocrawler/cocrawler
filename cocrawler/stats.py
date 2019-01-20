'''
A trivial stats system for CoCrawler
'''

import logging
import pickle
import time
from contextlib import contextmanager
import itertools

from hdrh.histogram import HdrHistogram
from sortedcollections import ValueSortedDict

from .urls import URL
from . import config
from . import memory

LOGGER = logging.getLogger(__name__)

start_time = time.time()
start_cpu = time.process_time()
maxes = {}
sums = {}
sets = {}
burners = {}
latencies = {}
coroutine_states = {}
exitstatus = 0


def init():
    memory.register_debug(mymemory)


def stats_max(name, value):
    maxes[name] = max(maxes.get(name, value), value)


def stats_sum(name, value, url=None):
    sums[name] = sums.get(name, 0) + value

    if url is not None:
        # remember these urls per-host
        # sum these sums per-host
        pass

    return sums[name]


def stats_set(name, value):
    sets[name] = value


def record_a_burn(name, start, url=None):
    if isinstance(url, URL):
        url = url.url
    elapsed = time.process_time() - start
    burn = burners.get(name, {})
    burn['count'] = burn.get('count', 0) + 1
    burn['time'] = burn.get('time', 0.0) + elapsed
    avg = burn.get('avg', 10000000.)

    # are we exceptional? 10x current average and significant
    if elapsed > avg * 10 and elapsed > 0.015:
        if 'list' not in burn:
            burn['list'] = ValueSortedDict()
        url = url or 'none'
        burn['list'][url] = -elapsed
        length = len(burn['list'])
        for _ in range(10, length):
            burn['list'].popitem()

    burn['avg'] = burn['time']/burn['count']
    burners[name] = burn


def record_a_latency(name, start, url=None, elapsedmin=10.0):
    if isinstance(url, URL):
        url = url.url
    elapsed = time.time() - start
    latency = latencies.get(name, {})
    latency['count'] = latency.get('count', 0) + 1
    latency['time'] = latency.get('time', 0.0) + elapsed
    if 'hist' not in latency:
        latency['hist'] = HdrHistogram(1, 30 * 1000, 2)  # 1ms-30sec, 2 sig figs
    latency['hist'].record_value(elapsed * 1000)  # ms

    if elapsed > elapsedmin:
        if 'list' not in latency:
            latency['list'] = ValueSortedDict()
        url = url or 'none'
        length = len(latency['list'])
        if length > 9:
            for u in itertools.islice(latency['list'], 9, length):
                del latency['list'][u]
        latency['list'][url] = -elapsed

    latencies[name] = latency


def update_cpu_burn(name, count, t, l):
    burn = burners.get(name, {})
    burn['count'] = burn.get('count', 0) + count
    burn['time'] = burn.get('time', 0.0) + t
    if l is not None:
        l = ValueSortedDict(l)
        burn['list'] = burn.get('list', ValueSortedDict())
        for k in l:  # XXX replace this loop with .update()
            burn['list'][k] = l[k]
        length = len(burn['list'])
        for _ in range(10, length):
            burn['list'].popitem()
    burners[name] = burn


@contextmanager
def record_burn(name, url=None):
    try:
        start = time.process_time()
        yield
    finally:
        record_a_burn(name, start, url=url)


@contextmanager
def record_latency(name, url=None, elapsedmin=10.0):
    try:
        start = time.time()
        yield
    finally:
        record_a_latency(name, start, url=url, elapsedmin=elapsedmin)


@contextmanager
def coroutine_state(k):
    # the documentation for generators leaves something to be desired
    coroutine_states[k] = coroutine_states.get(k, 0) + 1
    try:
        yield
    finally:
        coroutine_states[k] -= 1


def report():
    LOGGER.info('Stats report:')
    for s in sorted(sums):
        LOGGER.info('  %s: %d', s, sums[s])
    for s in sorted(maxes):
        LOGGER.info('  %s: %d', s, maxes[s])
    for s in sorted(sets):
        LOGGER.info('  %s: %d', s, sets[s])

    LOGGER.info('CPU burn report:')
    for key, burn in sorted(burners.items(), key=lambda x: x[1]['time'], reverse=True):
        LOGGER.info('  %s has %d calls taking %.3f cpu seconds.', key, burn['count'], burn['time'])
        if burn.get('list'):
            LOGGER.info('    biggest burners')
            for url in list(burn['list'].keys())[0:10]:
                e = - burn['list'][url]
                LOGGER.info('      %.3fs: %s', float(e), url)

    LOGGER.info('Latency report:')
    for key, latency in sorted(latencies.items(), key=lambda x: x[1]['time'], reverse=True):
        LOGGER.info('  %s has %d calls taking %.3f clock seconds.', key, latency['count'], latency['time'])
        t50 = latency['hist'].get_value_at_percentile(50.0) / 1000.
        t90 = latency['hist'].get_value_at_percentile(90.0) / 1000.
        t95 = latency['hist'].get_value_at_percentile(95.0) / 1000.
        t99 = latency['hist'].get_value_at_percentile(99.0) / 1000.
        stats_set('fetch 50', t50)
        stats_set('fetch 90', t90)
        stats_set('fetch 95', t95)
        stats_set('fetch 99', t99)
        LOGGER.info('  %s 50/90/95/99%%tiles are: %.2f/%.2f/%.2f/%.2f seconds', key, t50, t90, t95, t99)

        if latency.get('list'):
            LOGGER.info('    biggest latencies')
            for url in list(latency['list'].keys())[0:10]:
                e = - latency['list'][url]
                LOGGER.info('      %.3fs: %s', float(e), url)

    LOGGER.info('Summary:')
    elapsed = time.time() - start_time
    elapsedc = time.process_time() - start_cpu
    LOGGER.info('  Elapsed time is %.3f seconds', elapsed)
    LOGGER.info('  Main thread cpu time is %.3f seconds', elapsedc)
    stats_set('elapsed', elapsed)  # so it can be used for testing
    if elapsed > 0:
        LOGGER.info('  Main thread cpu %.1f%%', elapsedc/elapsed*100)
    bt = burners.get('burner thread parser total cpu time', {}).get('time', 0.)
    if bt > 0:
        LOGGER.info('  Burner thread burned %.3f cpu seconds', bt)
    if sums.get('fetch URLs', 0) and elapsed > 0:
        LOGGER.info('  Crawl rate is %d pages/second', int(sums['fetch URLs']/elapsed))
    if sums.get('fetch URLs', 0) and elapsedc > 0:
        LOGGER.info('  Crawl rate is %d pages/main-thread-cpu-second', int(sums['fetch URLs']/elapsedc))
    if sums.get('fetch bytes', 0) and elapsed > 0:
        LOGGER.info('  Crawl rate is %.2f gigabits/s', sums['fetch bytes']/elapsed*8/1000000000.)


def coroutine_report():
    LOGGER.info('Coroutine state:')
    for k, v in sorted(coroutine_states.items()):
        LOGGER.info('  %s: %d', k, v)


def stat_value(name):
    if name in maxes:
        return maxes[name]
    if name in sums:
        return sums[name]
    if name in sets:
        return sets[name]
    if name in burners:
        return burners[name].get('time', 0)
    # note, not including latency
    if name in coroutine_states:
        return coroutine_states[name]
    return None


def burn_values(name):
    if name in burners:
        return burners[name].get('time', 0), burners[name].get('count', 0)
    else:
        return None, None


def check_collisions():
    # see stat_value above -- these namespaces are shared when read
    keys = []
    keys.extend(maxes.keys())
    keys.extend(sums.keys())
    keys.extend(sets.keys())
    keys.extend(burners.keys())
    keys.extend(coroutine_states.keys())
    dedup = set()
    for k in keys:
        if k in dedup:
            LOGGER.error('stat name %s is used in multiple stats', k)
            global exitstatus
            exitstatus = 1
        dedup.add(k)


def check(no_test=False):
    if no_test:
        return

    seq = config.read('Testing', 'StatsEQ')
    sge = config.read('Testing', 'StatsGE')
    global exitstatus
    if seq:
        for s in seq:
            if stat_value(s) is None and seq[s] == 0:
                continue
            elif stat_value(s) is None:
                LOGGER.error('Stat %s does not exist, should be == %s', s, seq[s])
                exitstatus = 1
            elif stat_value(s) != seq[s]:
                LOGGER.error('Stat %s=%s is not the expected value of %s', s, stat_value(s), seq[s])
                exitstatus = 1
            else:
                LOGGER.debug('Stat %s=%s is the expected value', s, seq[s])
    if sge:
        for s in sge:
            if stat_value(s) is None and sge[s] == 0:
                continue
            elif stat_value(s) is None:
                LOGGER.error('Stat %s does not exist, should be >= %s', s, sge[s])
                exitstatus = 1
            elif stat_value(s) < sge[s]:
                LOGGER.error('Stat %s=%s is not >= %s', s, stat_value(s), sge[s])
                exitstatus = 1
            else:
                LOGGER.debug('Stat %s=%s is >= %s', s, stat_value(s), sge[s])


def burners_to_boring():
    d = dict()
    for k in burners:
        d[k] = burners[k]
        d[k]['list'] = dict(burners[k].get('list', dict()))
    return d


def boring_to_burners(d):
    global burners
    for k in d:
        burners[k] = d[k]
        burners[k]['list'] = ValueSortedDict(d[k].get('list', dict()))


def raw():
    '''
    Return a list of stuff suitable to feeding to stats.update() in a different thread.
    As a wart, ValueSortedDict can't be pickled. Turn it into a dict.
    '''
    d = burners_to_boring()

    # note, not including latency
    return maxes, sums, d


def update(l):
    '''
    Update current thread's stats with stats from elsewhere
    '''
    m, s, b = l
    for k in m:
        stats_max(k, m[k])
    for k in s:
        stats_sum(k, s[k])
    for k in b:
        update_cpu_burn(k, b[k]['count'], b[k]['time'], b[k].get('list'))
    stats_set('parser cpu time', burners.get('burner thread parser total cpu time', {}).get('time', 0))


def clear():
    '''
    After raw(), this clears stats to prevent double-counting
    '''
    global maxes
    maxes = {}
    global sums
    sums = {}
    for b in burners:
        burners[b] = {'avg': burners[b].get('avg'), 'count': 0, 'time': 0}
    for l in latencies:
        latencies[l] = {'avg': latencies[l].get('avg'), 'count': 0, 'time': 0}


def save(f):
    pickle.dump('stats', f)
    pickle.dump(start_time, f)
    pickle.dump(burners_to_boring(), f)
    pickle.dump(maxes, f)
    pickle.dump(sums, f)


def load(f):
    if pickle.load(f) != 'stats':
        raise ValueError('invalid stats section in savefile')
    global start_time
    start_time = pickle.load(f)
    boring = pickle.load(f)
    boring_to_burners(boring)
    global maxes
    maxes = pickle.load(f)
    global sums
    sums = pickle.load(f)


def mymemory():
        maxes = {}
        maxes['bytes'] = memory.total_size(maxes)
        maxes['len'] = len(maxes)
        sums = {}
        sums['bytes'] = memory.total_size(sums)
        sums['len'] = len(sums)
        sets = {}
        sets['bytes'] = memory.total_size(sets)
        sets['len'] = len(sets)
        burners = {}
        burners['bytes'] = memory.total_size(burners)
        burners['len'] = len(burners)
        latencies = {}
        latencies['bytes'] = memory.total_size(latencies)
        latencies['len'] = len(latencies)
        coroutine_stats = {}
        coroutine_stats['bytes'] = memory.total_size(coroutine_stats)
        coroutine_stats['len'] = len(coroutine_stats)
        return {'maxes': maxes, 'sums': sums, 'sets': sets, 'burners': burners,
                'latencies': latencies, 'coroutine_stats': coroutine_stats}
