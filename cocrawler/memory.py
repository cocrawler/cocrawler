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

from sys import getsizeof, stderr
from itertools import chain
from collections import deque
try:
    from reprlib import repr
except ImportError:
    pass

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
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    except ValueError as e:  # should always work, but hey, MacOS, you do you
        LOGGER.warning('Failed to set RLIMIT_NOFILE to %d, got %s', hard, str(e))
        new_soft = 10240  # OPEN_MAX, the secret MacOS limit in the kernel
        if new_soft > soft:
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft, hard))
                LOGGER.warning('Fallback to set RLIMIT_NOFILE to %d worked', new_soft)
            except ValueError as e:
                LOGGER.warning('Fallback to set RLIMIT_NOFILE to %d also got %s', new_soft, str(e))
    # XXX warn if too few compared to max_wokers?

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


# https://code.activestate.com/recipes/577504/ -- MIT license
def total_size(o, handlers={}, verbose=False):
    """ Returns the approximate memory footprint an object and all of its contents.

    Automatically finds the contents of the following builtin containers and
    their subclasses:  tuple, list, deque, dict, set and frozenset.
    To search other containers, add handlers to iterate over their contents:

        handlers = {SomeContainerClass: iter,
                    OtherContainerClass: OtherContainerClass.get_elements}

    """
    dict_handler = lambda d: chain.from_iterable(d.items())
    all_handlers = {tuple: iter,
                    list: iter,
                    deque: iter,
                    dict: dict_handler,
                    set: iter,
                    frozenset: iter,
                   }
    all_handlers.update(handlers)     # user handlers take precedence
    seen = set()                      # track which object id's have already been seen
    default_size = getsizeof(0)       # estimate sizeof object without __sizeof__

    def sizeof(o):
        if id(o) in seen:       # do not double count the same object
            return 0
        seen.add(id(o))
        s = getsizeof(o, default_size)

        if verbose:
            print(s, type(o), repr(o), file=stderr)

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)
