import logging
import asyncio
from concurrent.futures import ProcessPoolExecutor
import functools

import stats

LOGGER = logging.getLogger(__name__)

def stats_wrap(partial, name):
    stats.clear()
    with stats.record_burn(name):
        ret = list(partial()) # XXX what's pythonic here?
    s = stats.raw()
    return s, ret

class Burner:
    '''
    Use threads for cpu-burning stuff, monitored by async coroutines.

    On my test machine, it takes about 0.5 milliseconds in the main async
    thread for a single call to the burner thread. That was without any
    affinity.

    TODO: CPU affinity?
    '''
    def __init__(self, thread_count, loop, name):
        self.executor = ProcessPoolExecutor(thread_count)
        self.loop = loop
        self.name = name
        # XXX add some more strings that report will eventually use

    async def burn(self, partial):
        '''
        Do some cpu burning, and record stats related to it.

        Use functools.partial to wrap up your work function and its args.
        '''
        wrap = functools.partial(stats_wrap, partial, 'burner thread {} total cpu time'.format(self.name))

        f = asyncio.ensure_future(self.loop.run_in_executor(self.executor, wrap))
        with stats.coroutine_state('await burner thread {}'.format(self.name)):
            s, l = await f

        stats.update(s)
        return l

    def report(self):
        # how much work/cpu-sec did I do?
        # what was the 100/90/50/10/0 %tile of cpu time in a burn
        return
