'''
Code related to the crawler timer thread... mostly stats stuff.

TODO: add methods to add one-shot and periodic stuff dynamically

Hardwired stuff: "fast" and "slow" set to 1 second and 30 seconds

Record delta, Send stats to Carbon

TODO rebin to actual "fast" and "slow" durations

The split between recording and pushing stats tolerates hiccups
better than doing both together.
'''

import pickle
import struct
import time

import asyncio

import stats

fast_prefix = 'cocrawler'

fast_stats = [
    {'name': 'DNS prefetches', 'kind': 'persec', 'qps_total': True},
    {'name': 'fetch URLs', 'kind': 'persec', 'qps_total': True},
    {'name': 'robots fetched', 'kind': 'persec', 'qps_total': True},
    {'name': 'fetch bytes', 'kind': 'persec', 'normalize': 8/1000000000.},
    {'name': 'await burner thread parser', 'kind': 'value'},
    {'name': 'fetcher fetching', 'kind': 'value'},
    {'name': 'fetcher retry sleep', 'kind': 'value'},
    {'name': 'fetching/checking robots', 'kind': 'value'},
    {'name': 'robots collision sleep', 'kind': 'value'},
    {'name': 'priority', 'kind': 'value'},
#    {'name': 'fetch 50', 'kind': 'value'},
#    {'name': 'fetch 90', 'kind': 'value'},
#    {'name': 'fetch 95', 'kind': 'value'},
#    {'name': 'fetch 99', 'kind': 'value'},
]

slow_prefix = 'cocrawler'

slow_stats = [
]

ft = None
st = None

def start_carbon(loop, config):
    server = config['CarbonStats'].get('Server', 'localhost')
    port = int(config['CarbonStats'].get('Port', '2004'))

    global ft
    fast = CarbonTimer(1, fast_prefix, fast_stats, server, port, loop)
    ft = asyncio.Task(fast.timer(), loop=loop)

    global st
    slow = CarbonTimer(30, slow_prefix, slow_stats, server, port, loop)
    st = asyncio.Task(slow.timer(), loop=loop)

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
        r, w = await asyncio.open_connection(host=server, port=port, loop=loop)
        w.write(message)
        await w.drain()
        w.close()
    except OSError:
        # XXX do something useful here
        stats.stats_sum('carbon stats push fail', 1)
        pass

class CarbonTimer:
    def __init__(self, dt, prefix, stats_list, server, port, loop):
        self.dt = dt
        self.prefix = prefix
        self.stats_list = stats_list
        self.server = server
        self.port = port
        self.loop = loop

    async def timer(self):
        self.last_t = time.time()
        self.last = None

        while True:
            await asyncio.sleep(self.last_t + self.dt - time.time())
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
                    if s['kind'] == 'persec':
                        value = (new[n] - self.last[n])/elapsed
                    elif s['kind'] == 'value':
                        value = new[n]
                    value *= s.get('normalize', 1.0)
                    if s.get('qps_total'):
                        qps_total += value
                    path = '{}.{}.{}'.format(self.prefix, s['kind'], n.replace('/', '_').replace(' ', '_'))
                    carbon_tuples.append((path, (t, value)))
                carbon_tuples.append((self.prefix+'.persec.qps_total', (t, qps_total)))
                carbon_tuples.append((self.prefix+'.persec.elapsed', (t, elapsed)))

                await carbon_push(self.server, self.port, carbon_tuples, self.loop)

            self.last = new
            self.last_t = t
