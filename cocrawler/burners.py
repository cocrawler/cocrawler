'''
Python's multiprocess library doesn't work with async/await.

Given that I am lazy, this file implements a similarish thing as a RESTish
server. This module starts the server(s) and is the client.

The best usage of this is to have a single call do a lot of processing, and
return a lot of results. For example, all of the post-crawl html parsing to
extract urls, detecting facets like facebook opengraph, checking urls against
blocklists, etc. Then pass back everything in a big json thingie.

TODO: propagate stats back to the parent?

So far this service is way too slow -- maybe 7 MB/elapsed second. I
don't think it helps that we have to go str -> bytes -> str, over the
loopback, all that crap. Straight passing through memory (like
multiprocessing does) has got to be faster.
'''

import os
import subprocess
import time
import logging
import json
import asyncio
import aiohttp
import signal

import psutil

import pdeathsig
import stats

LOGGER = logging.getLogger(__name__)

cpuset = os.sched_getaffinity(0)
portset = set(range(6800, 6900))
portsetset = False

class Burner:
    def __init__(self, name, count, config):
        self.ports = []
        self.popens = []
        self.next_poll = 0
        self.poll_interval = 5

        pr = config.get('PortRange')
        if pr:
            start, end = pr.split('-', maxsplit=1)
            # omg this is so ugly.
            if not portsetset:
                global portset
                portset = range(start, end+1)
                portsetset = True

        # negative count means leave that many cpus free
        if count < 0:
            cpus = psutil.cpu_count() # we are OK with logical=True, I guess.
            count = cpus + count
            if count > len(cpuset):
                # if affinity is off and there are 2+ burners this won't work right. whatever.
                LOGGER.warning('CPU oversubscription spotted in restburn %s', name)

        ua = config.get('UseAffinity')
        if ua:
            uc = config.get('UsedCPUs')
            if uc:
                for cpu in uc:
                    cpuset.discard(cpu)

        for i in range(count):
            args = ['python', name]
            port = portset.pop()
            self.ports.append(port)
            args.extend(['--port', str(port)])
            if ua:
                cpu = cpuset.pop()
                args.extend(['--cpu', str(cpu)])

            try:
                po = subprocess.Popen(args, preexec_fn=lambda: pdeathsig.set_pdeathsig(signal.SIGTERM))
            except (OSError, ValueError) as e:
                LOGGER.error('Error launching burner process: %r', e)
                LOGGER.error(' Burner args were: %r', args)
                os.exit(1)

            self.popens.append((po, port, args))
            LOGGER.info('started a %s on port %d', name, port)
            if ua:
                LOGGER.info(' (cpu was %d)', cpu)
        time.sleep(1) # give daemons a chance to get going

    def poll(self, force=False):
        t = time.time()
        if t > self.next_poll or force:
            for po, port, args in self.popens.copy():
                ret = po.poll()

                if ret is not None:
                    LOGGER.info('Burner process for port %d exited with returncode of %d', port, ret)
                    self.popens.remove((po, port, args))
                    try:
                        po = subprocess.Popen(args, preexec_fn=lambda: prctl(PDEATHSIG, sig.SIGTERM))
                        self.popens.append((po, port, args))
                        time.sleep(1) # give it a chance to start up
                    except (OSError, ValueError) as e:
                        LOGGER.error('Exception on restart of burner: %r', e)
                        LOGGER.error(' Burner args were: %r', args)
                        LOGGER.error(' not retrying restart.')

            self.next_poll = t + self.poll_interval

    def kill(self):
        for po, port, args in self.popens:
            po.kill()
        self.popens = []
        # does not return cpus or ports, so, yeah

    async def post(self, name, string, timeout=30):
        '''
        POSTs string to one of the burners to have 'name' done to it.
        Maybe I should allow json, too?
        Return is expected to be json.
        '''
        self.poll()

        tries = 0
        while True:
            tries += 1
            if tries > 10:
                LOGGER.error('Too many retries for burner %s, returning None', name)
                self.poll(force=True)
                return

            # rotate the list
            po, port, args = self.popens.pop(0)
            self.popens.append((po, port, args))

            url = 'http://localhost:{}/{}/'.format(port, name)
            response = None

            try:
                with stats.coroutine_state('restburn '+name):
                    with aiohttp.Timeout(timeout):
                        with aiohttp.ClientSession() as session:
                            print('length of string is {}'.format(len(string)))
                            response = await session.post(url, data={'data':string})
                            body_bytes = await response.read()
                if response.status < 500:
                    break
            except aiohttp.ClientResponseError:
                break
            except aiohttp.ClientOSError:
                # these are things like 'can't connect' i.e. process startup
                await asyncio.sleep(1)
                # silently retry
            except Exception as e:
                LOGGER.warn('Surprised by burner %s port=%d fetch getting exception=%r,'
                            ' retrying', name, port, e)
                await asyncio.sleep(1)

        if response and response.status == 200:
            body = await response.text() # we need the unicode, not the bytes
            j = json.loads(body)
            return j.get('answer', {})

        LOGGER.warn('Surprised by burner %s returning status %d, not retrying', name, response.status)
        return {}
