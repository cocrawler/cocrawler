'''
Code related to memory and memory debugging
'''

import logging
import io
import resource

import objgraph

from . import config

LOGGER = logging.getLogger(__name__)

debugs = []


def register_debug(cref):
    '''
    Register a function to be called for a memory summary
    '''
    debugs.append(cref)


def _in_millions(m):
    return '{:.1f}mb'.format(m / 1000000.)


def print_summary():
    '''
    Log a summary of current memory usage
    '''
    if not config.read('Crawl', 'DebugMemory'):
        return

    mem = {}
    for d in debugs:
        mem.update(d())

    LOGGER.info('Memory summary:')

    for k in sorted(mem.keys()):
        v = mem[k]
        LOGGER.info('  %s len %d bytes %s', k, v['len'], _in_millions(v['bytes']))

    LOGGER.info('Top objects:')

    lines = io.StringIO()
    objgraph.show_most_common_types(limit=20, file=lines)
    lines.seek(0)
    for l in lines.read().splitlines():
        LOGGER.info('  %s', l)


def limit_resources():
    _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    # XXX warn if too few compared to max_wokers?
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))

    _, hard = resource.getrlimit(resource.RLIMIT_AS)
    rlimit_as = int(config.read('System', 'RLIMIT_AS_gigabytes'))
    if rlimit_as == 0:
        return
    LOGGER.info('Setting RLIMIT_AS per configuration to %d gigabytes', rlimit_as)
    rlimit_as *= 1024 * 1024 * 1024
    if hard > 0 and rlimit_as > hard:
        LOGGER.error('RLIMIT_AS limited to %d bytes by system limit', hard)
        rlimit_as = hard
    resource.setrlimit(resource.RLIMIT_AS, (rlimit_as, hard))
