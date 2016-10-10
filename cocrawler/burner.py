import logging
import time
import asyncio
from concurrent.futures import ProcessPoolExecutor
import functools

import psutil
import stats

LOGGER = logging.getLogger(__name__)

def stats_wrap(partial, name, url=None):
    '''
    Helper function to propagate stats back to main thread
    '''
    stats.clear()
    with stats.record_burn(name, url=url):
        ret = list(partial()) # XXX what's pythonic here?
    s = stats.raw()
    return s, ret

def set_an_affinity(cpu):
    '''
    Run in a burner thread to set affinity. The sleep
    allows us to set affinity for each burner thread.
    '''
    p = psutil.Process()
    p.cpu_affinity([cpu])
    time.sleep(1) # no asyncio in this process

class Burner:
    '''
    Use threads for cpu-burning stuff, monitored by async coroutines.

    On my test machine, it takes about 0.5 milliseconds in the main async
    thread for a single call to the burner thread.
    '''
    def __init__(self, config, loop, name):
        thread_count = int(config['Multiprocess']['BurnerThreads'])
        self.executor = ProcessPoolExecutor(thread_count)
        self.loop = loop
        self.name = name

        if config['Multiprocess'].get('Affinity'):
            p = psutil.Process()
            all_cpus = p.cpu_affinity()
            for _ in range(thread_count):
                cpu = all_cpus.pop()
                wrap = functools.partial(set_an_affinity, cpu)
                f = asyncio.ensure_future(self.loop.run_in_executor(self.executor, wrap))

    async def burn(self, partial, url=None):
        '''
        Do some cpu burning, and record stats related to it.

        Use functools.partial to wrap up your work function and its args.
        '''
        wrap = functools.partial(stats_wrap, partial, 'burner thread {} total cpu time'.format(self.name), url=url)

        f = asyncio.ensure_future(self.loop.run_in_executor(self.executor, wrap))
        with stats.coroutine_state('await burner thread {}'.format(self.name)):
            s, l = await f

        stats.update(s)
        return l

    def report(self):
        return
