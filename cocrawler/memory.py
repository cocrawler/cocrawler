'''
Code related to memory and memory debugging
'''

import logging
import io
import resource
import gc
import os
import random
import tempfile

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


def print_objects(f):
    gc.collect()
    with open(f, 'r') as fd:
        for line in fd:
            line = line.strip()
            try:
                obj = random.choice(objgraph.by_type(line))
            except Exception as e:
                LOGGER.info('exception trying to objgraph a random %s: %s', line, str(e))
                break
            with tempfile.NamedTemporaryFile(dir='/tmp', prefix=line, suffix='.dot', mode='w') as out:
                try:
                    objgraph.show_chain(objgraph.find_backref_chain(obj, objgraph.is_proper_module), output=out)
                    LOGGER.info('object %s file %s', line, out.name)
                except Exception as e:
                    LOGGER.info('exception trying to show_chain a random %s: %s', line, str(e))
    try:
        os.remove(f)
    except Exception as e:
        LOGGER.info('exception %s removing memory_crawler file %s', str(e), f)


def print_summary(f):
    '''
    Log a summary of current memory usage. This is very expensive
    when there is a lot of memory used.
    '''
    if os.path.isfile(f):
        print_objects(f)

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

    gc.collect()
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
