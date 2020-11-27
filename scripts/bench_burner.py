import functools
import time
import argparse
import sys

import asyncio

import cocrawler.burner as burner
import cocrawler.config as config


loop = asyncio.get_event_loop()
b = None
queue = asyncio.Queue()


def burn(dt, data):
    t0 = time.process_time()
    end = t0 + dt
    while time.process_time() < end:
        pass
    return 1,


async def work():
    while True:
        dt, data = await queue.get()
        partial = functools.partial(burn, dt, data)
        await b.burn(partial)
        queue.task_done()


async def crawl():
    workers = [asyncio.Task(work(), loop=loop) for _ in range(100)]
    await queue.join()
    for w in workers:
        if not w.done():
            w.cancel()


def main():
    ARGS = argparse.ArgumentParser(description='bench_burn benchmark for burner thread overhead')
    ARGS.add_argument('--threads', type=int, default=2)
    ARGS.add_argument('--workers', type=int, default=100)
    ARGS.add_argument('--datasize', type=int, default=10000)
    ARGS.add_argument('--affinity', action='store_true')
    ARGS.add_argument('--duration', type=float, default=0.010)
    ARGS.add_argument('--count', type=int, default=10000)
    args = ARGS.parse_args()

    c = {'Multiprocess': {'BurnerThreads': args.threads, 'Affinity': args.affinity}}
    config.set_config(c)
    global b
    b = burner.Burner('parser')

    for _ in range(args.count):
        queue.put_nowait((args.duration, 'x' * args.datasize))

    print('args are', args)

    print('Processing {} items of size {} kbytes and {:.3f} seconds of burn using {} burner threads'.format(
        args.count, int(args.datasize/1000), args.duration, args.threads))

    t0 = time.time()
    c0 = time.process_time()

    try:
        loop.run_until_complete(crawl())
    except KeyboardInterrupt:
        sys.stderr.flush()
        print('\nInterrupt. Exiting.\n')
    finally:
        loop.stop()
        loop.run_forever()
        loop.close()

    elapsed = time.time() - t0
    print('Elapsed time is {:.1f} seconds.'.format(elapsed))
    expected = args.count * args.duration / args.threads
    print('Expected is {:.1f} seconds.'.format(expected))

    print('Burner-side overhead is {}% or {:.4f} seconds per call'.format(
        int((elapsed - expected)/expected*100), (elapsed - expected)/args.count))

    celapsed = time.process_time() - c0
    print('Main-thread overhead is {}%, {:.4f} seconds per call, {} calls per cpu-second'.format(
        int(celapsed/elapsed*100), celapsed/args.count, int(args.count/celapsed)))


if __name__ == '__main__':
    # this guard needed for MacOS and Windows
    main()
