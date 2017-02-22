'''
Code related to the crawler timer thread... mostly stats stuff.

TODO: add methods to add one-shot and periodic stuff dynamically

Hardwired stuff: "fast" and "slow" set to 1 second and 30 seconds

Record delta, Send stats to Carbon

TODO rebin to actual "fast" and "slow" durations, instead of my 29 second hack
 (which is failing to eliminate gaps in the .slow. stats)

TODO option to spit out text, since setting up carbon/graphite is kinda annoying

TODO The split between recording and pushing stats tolerates hiccups
better than doing both together.
'''

import pickle
import struct
import time
import resource
import logging

import asyncio

from . import stats
from . import timebin

LOGGER = logging.getLogger(__name__)

fast_prefix = 'cocrawler.fast'

fast_stats = [
    {'name': 'DNS prefetches', 'kind': 'delta', 'qps_total': True},
    {'name': 'fetch URLs', 'kind': 'delta', 'qps_total': True},
    {'name': 'robots fetched', 'kind': 'delta', 'qps_total': True},
    {'name': 'fetch bytes', 'kind': 'delta', 'normalize': 8/1000000000.},
    {'name': 'awaiting work'},
    {'name': 'await burner thread parser'},
    {'name': 'await main thread parser'},
    {'name': 'fetcher fetching'},
    {'name': 'fetcher retry sleep'},
    {'name': 'fetching/checking robots'},
    {'name': 'robots collision sleep'},
    {'name': 'fetcher DNS lookup'},
    {'name': 'priority'},
]

slow_prefix = 'cocrawler.slow'

slow_stats = [
    {'name': 'initial seeds'},
    {'name': 'added seeds'},
    {'name': 'fetch URLs'},
    {'name': 'fetch bytes', 'normalize': 1/1000000000.},
    {'name': 'robots denied'},
    {'name': 'tries completely exhausted'},
    {'name': 'max queue size'},
    {'name': 'queue size'},
    {'name': 'added urls'},
    {'name': 'parser cpu time', 'kind': 'delta'},
    {'name': 'main thread cpu time', 'kind': 'delta'},
]


async def exception_wrapper(partial, name):
    try:
        await partial()
    except asyncio.CancelledError:
        # this happens during teardown
        pass
    except Exception as e:
        LOGGER.error('timer %s threw an exception %r', name, e)

ft = None
st = None


def start_carbon(loop, config):
    server = config['CarbonStats'].get('Server', 'localhost')
    port = int(config['CarbonStats'].get('Port', '2004'))

    global ft
    fast = CarbonTimer(1, fast_prefix, fast_stats, server, port, loop)
    ft = asyncio.Task(exception_wrapper(fast.timer, 'fast carbon timer'), loop=loop)

    global st
    slow = CarbonTimer(30, slow_prefix, slow_stats, server, port, loop)
    st = asyncio.Task(exception_wrapper(slow.timer, 'slow carbon timer'), loop=loop)


def close():
    if not ft.done():
        ft.cancel()
    if not st.done():
        st.cancel()


async def carbon_push(server, port, tuples, loop):
    payload = pickle.dumps(tuples, protocol=2)
    header = struct.pack("!L", len(payload))
    message = header + payload
    try:
        _, w = await asyncio.open_connection(host=server, port=port, loop=loop)
        w.write(message)
        await w.drain()
        w.close()
    except OSError as e:
        LOGGER.warn('carbon stats push fail: %r', e)
        stats.stats_sum('carbon stats push fail', 1)


class CarbonTimer:
    def __init__(self, dt, prefix, stats_list, server, port, loop):
        self.dt = dt
        self.prefix = prefix
        self.stats_list = stats_list
        self.server = server
        self.port = port
        self.loop = loop
        self.last_t = None
        self.last = None
        for sl in stats_list:
            sl['timebin'] = timebin.TimeBin(dt)
        self.qps_timebin = timebin.TimeBin(dt)
        self.elapsed_timebin = timebin.TimeBin(dt)
        self.vmem_timebin = timebin.TimeBin(dt)

    async def timer(self):
        self.last_t = time.time()
        self.last = None

        while True:
            # make this expire just after a timebin boundary
            deadline = (int(self.last_t / self.dt) + 1) * self.dt + 0.001
            await asyncio.sleep(deadline - time.time())
            t = time.time()
            elapsed = t - self.last_t

            new = {}
            for s in self.stats_list:
                n = s['name']
                new[n] = stats.stat_value(n)

            if self.last:
                qps_total = 0
                carbon_tuples = []
                for s in self.stats_list:
                    n = s['name']
                    if s.get('kind', '') == 'delta':
                        value = (new[n] - self.last[n])/elapsed
                    else:
                        value = new[n]
                    value *= s.get('normalize', 1.0)
                    if s.get('qps_total'):
                        qps_total += value
                    tb = s['timebin']
                    tb.point(t, value)
                    path = '{}.{}'.format(self.prefix, n.replace('/', '_').replace(' ', '_'))
                    carbon_tuples += tb.gettuples(path)

                self.qps_timebin.point(t, qps_total)
                carbon_tuples += self.qps_timebin.gettuples(self.prefix+'.qps_total')
                self.elapsed_timebin.point(t, elapsed)
                carbon_tuples += self.elapsed_timebin.gettuples(self.prefix+'.elapsed')

                ru = resource.getrusage(resource.RUSAGE_SELF)
                vmem = (ru[2])/1000000.
                # TODO: swapouts in 8, blocks out in 10
                self.vmem_timebin.point(t, vmem)
                carbon_tuples += self.vmem_timebin.gettuples(self.prefix+'.vmem')

                if carbon_tuples:
                    await carbon_push(self.server, self.port, carbon_tuples, self.loop)

            self.last = new
            self.last_t = t
