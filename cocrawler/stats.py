'''
A trivial stats system for CoCrawler
'''

import logging

import time

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
        LOGGER.info('  %s: %d', s, sums[s])
    for s in sorted(maxes):
        LOGGER.info('  %s: %d', s, maxes[s])

    LOGGER.info('CPU burn report:')
    for key, burn in sorted(burners.items(), key=lambda x: x[1]['time'], reverse=True):
        LOGGER.info('  %s has %d calls taking %.3f cpu seconds.', key, burn['count'], burn['time'])

    LOGGER.info('Summary:')
    elapsed = time.time() - start_time
    LOGGER.info('  Elapsed time is %.3f seconds', elapsed)
    if sums.get('URLs fetched', 0) and elapsed > 0:
        LOGGER.info('  Crawl rate is %d pages/second', int(sums['URLs fetched']/elapsed))

def stat_value(name):
    if name in sums:
        return sums[name]
    if name in maxes:
        return maxes[name]
    return None

def check(config):
    seq = config.get('Testing', {}).get('StatsEQ', {})
    global exitstatus
    if seq:
        for s in seq:
            if stat_value(s) != seq[s]:
                LOGGER.error('Stat %s=%s is not the expected value of %s', s, stat_value(s), seq[s])
                exitstatus = 1
            else:
                LOGGER.debug('Stat %s=%s is the expected value', s, seq[s])

