#!/usr/bin/env python

'''
CoCrawler web crawler, main program
'''
import sys
import os
import faulthandler
import gc

import argparse
import asyncio
import logging
import warnings

import cocrawler
import cocrawler.config as config
import cocrawler.stats as stats
import cocrawler.timer as timer
import cocrawler.webserver as webserver
import cocrawler.memory as memory

LOGGER = logging.getLogger(__name__)

faulthandler.enable()

ARGS = argparse.ArgumentParser(description='CoCrawler web crawler')
ARGS.add_argument('--config', action='append')
ARGS.add_argument('--configfile', action='store')
ARGS.add_argument('--no-test', action='store_true', help='do not check stats at the end of crawling')
ARGS.add_argument('--printdefault', action='store_true', help='print the default configuration')
ARGS.add_argument('--printfinal', action='store_true', help='print the final configuration')
ARGS.add_argument('--load', action='store', help='load saved crawl')
ARGS.add_argument('--loglevel', action='store', default='INFO', help='set logging level, default INFO')
ARGS.add_argument('--verbose', '-v', action='count', help='set logging level to DEBUG')


def main():
    '''
    Main program: parse args, read config, set up event loop, run the crawler.
    '''

    args = ARGS.parse_args()

    if args.printdefault:
        config.print_default()
        sys.exit(1)

    loglevel = os.getenv('COCRAWLER_LOGLEVEL')
    if loglevel is None and args.loglevel:
        loglevel = args.loglevel
    if loglevel is None and args.verbose:
        loglevel = 'DEBUG'

    logging.basicConfig(level=loglevel)

    config.config(args.configfile, args.config)

    if args.printfinal:
        config.print_final()
        sys.exit(1)

    memory.limit_resources()

    if os.getenv('PYTHONASYNCIODEBUG') is not None:
        logging.captureWarnings(True)
        warnings.simplefilter('default', category=ResourceWarning)
        if LOGGER.getEffectiveLevel() > logging.WARNING:
            LOGGER.setLevel(logging.WARNING)
            LOGGER.warning('Lowered logging level to WARNING because PYTHONASYNCIODEBUG env var is set')
        LOGGER.warning('Configured logging system to show ResourceWarning because PYTHONASYNCIODEBUG env var is set')
        LOGGER.warning('Note that this does have a significant impact on asyncio overhead')
    if os.getenv('COCRAWLER_GC_DEBUG') is not None:
        LOGGER.warning('Configuring gc debugging')
        gc.set_debug(gc.DEBUG_STATS | gc.DEBUG_UNCOLLECTABLE)

    kwargs = {}
    if args.load:
        kwargs['load'] = args.load
    if args.no_test:
        kwargs['no_test'] = True

    crawler = cocrawler.Crawler(**kwargs)
    loop = asyncio.get_event_loop()
    slow_callback_duration = os.getenv('ASYNCIO_SLOW_CALLBACK_DURATION')
    if slow_callback_duration:
        loop.slow_callback_duration = float(slow_callback_duration)
        LOGGER.warning('set slow_callback_duration to %f', slow_callback_duration)

    if config.read('CarbonStats'):
        timer.start_carbon()

    if config.read('REST'):
        app = webserver.make_app()
    else:
        app = None

    try:
        loop.run_until_complete(crawler.crawl())
    except KeyboardInterrupt:
        sys.stderr.flush()
        print('\nInterrupt. Exiting cleanly.\n')
        crawler.cancel_workers()
    finally:
        loop.run_until_complete(crawler.close())
        if app:
            webserver.close(app)
        if config.read('CarbonStats'):
            timer.close()
        # vodoo recommended by advanced aiohttp docs for graceful shutdown
        # https://github.com/aio-libs/aiohttp/issues/1925
        loop.run_until_complete(asyncio.sleep(0.250))
        loop.close()


if __name__ == '__main__':
    main()
    exit(stats.exitstatus)
