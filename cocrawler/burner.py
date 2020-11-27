import logging
import time
import asyncio
import os
from concurrent.futures import ProcessPoolExecutor
import functools
import traceback

import psutil

from . import stats
from . import config

LOGGER = logging.getLogger(__name__)


def stats_wrap(partial, name, url=None):
    '''
    Helper function to propagate stats back to main thread
    '''
    stats.clear()
    with stats.record_burn(name, url=url):
        try:
            ret = list(partial())
        except Exception as e:
            stats.stats_sum('burner thread raised', 1)
            LOGGER.info('burner thread sees an exception %r', e)
            traceback.print_exc()
            ret = []
    s = stats.raw()
    return s, ret


def set_an_affinity(cpu):
    '''
    Run in a burner thread to set affinity. The sleep
    allows us to set affinity for each burner thread by
    racing.
    '''
    p = psutil.Process()
    p.cpu_affinity([cpu])
    time.sleep(1)  # no asyncio in this process


class Burner:
    '''
    Use threads for cpu-burning stuff, monitored by async coroutines.

    On my test machine, it takes about 0.5 milliseconds in the main async
    thread for a single call to the burner thread.
    '''
    def __init__(self, name):
        thread_count = int(config.read('Multiprocess', 'BurnerThreads'))
        self.executor = ProcessPoolExecutor(thread_count)
        self.loop = asyncio.get_event_loop()
        self.name = name
        self.f = []
        p = psutil.Process()
        has_cpu_affinity = hasattr(p, 'cpu_affinity')  # MacOS does not

        if not has_cpu_affinity:
            if config.read('Multiprocess', 'Affinity'):
                LOGGER.info('config asks for Multiprocess Affinity but this OS does not support it')
            if thread_count >= os.cpu_count():
                LOGGER.warning('fewer available cpus (%d) than burner threads (%d), performance will suffer',
                               os.cpu_count() - 1, thread_count)
        elif config.read('Multiprocess', 'Affinity'):
            all_cpus = p.cpu_affinity()
            for burner_no in range(thread_count):
                try:
                    cpu = all_cpus.pop()
                except IndexError:
                    LOGGER.error('Too few cpus available (%d) to set affinities for burner threads (%d)',
                                 len(p.cpu_affinity()), thread_count)
                    break
                wrap = functools.partial(set_an_affinity, cpu)
                LOGGER.info('setting cpu affinity of burner thread %d to core %d', burner_no, cpu)
                f = asyncio.ensure_future(
                    self.loop.run_in_executor(self.executor, wrap))  # pylint: disable=unused-variable
                self.f.append(f)
            # I can't await f because I'm not async
            # todo: "from asyncinit import asyncinit" and @asyncinit decorator
        else:
            if thread_count >= len(p.cpu_affinity()):
                LOGGER.warning('fewer available cpus (%d) than burner threads (%d), performance will suffer',
                               len(p.cpu_affinity()) - 1, thread_count)

    async def burn(self, partial, url=None):
        '''
        Do some cpu burning, and record stats related to it.

        Use functools.partial to wrap up your work function and its args.

        stats_wrap is used to report how much work was done in the burner threads.
        '''
        wrap = functools.partial(stats_wrap, partial,
                                 'burner thread {} total cpu time'.format(self.name), url=url)

        f = asyncio.ensure_future(self.loop.run_in_executor(self.executor, wrap))
        with stats.coroutine_state('await burner thread {}'.format(self.name)):
            s, l = await f

        stats.update(s)
        return l

    def report(self):
        return
