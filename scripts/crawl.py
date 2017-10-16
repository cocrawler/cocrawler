#!/usr/bin/env python

'''
CoCrawler web crawler, main program
'''
import sys
import resource
import os
import faulthandler

import argparse
import asyncio
import logging

import cocrawler
import cocrawler.config as config
import cocrawler.stats as stats
import cocrawler.timer as timer
import cocrawler.webserver as webserver

faulthandler.enable()

ARGS = argparse.ArgumentParser(description='CoCrawler web crawler')
ARGS.add_argument('--config', action='append')
ARGS.add_argument('--configfile', action='store')
ARGS.add_argument('--no-confighome', action='store_true')
ARGS.add_argument('--no-test', action='store_true')
ARGS.add_argument('--printdefault', action='store_true')
ARGS.add_argument('--loglevel', action='store', type=int, default=2)
ARGS.add_argument('--load', action='store')


def limit_resources():
    _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))  # XXX compare to max threads etc.

    _, hard = resource.getrlimit(resource.RLIMIT_AS)  # RLIMIT_VMEM does not exist?!
    resource.setrlimit(resource.RLIMIT_AS, (16 * 1024 * 1024 * 1024, hard))  # XXX config


def main():
    '''
    Main program: parse args, read config, set up event loop, run the crawler.
    '''

    args = ARGS.parse_args()
    try:
        loglevel = int(os.getenv('COCRAWLER_LOGLEVEL'))
    except (ValueError, TypeError):
        loglevel = args.loglevel

    if args.printdefault:
        config.print_default()
        sys.exit(1)

    levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
    logging.basicConfig(level=levels[min(loglevel, len(levels)-1)])

    config.config(args.configfile, args.config, confighome=not args.no_confighome)
    limit_resources()

    kwargs = {}
    if args.load:
        kwargs['load'] = args.load
    if args.no_test:
        kwargs['no_test'] = True

    crawler = cocrawler.Crawler(**kwargs)
    loop = asyncio.get_event_loop()

    if config.read('CarbonStats'):
        timer.start_carbon(loop)

    if config.read('REST'):
        app = webserver.make_app(loop)
    else:
        app = None

    try:
        loop.run_until_complete(crawler.crawl())
    except KeyboardInterrupt:
        sys.stderr.flush()
        print('\nInterrupt. Exiting cleanly.\n')
        stats.coroutine_report()
        crawler.cancel_workers()
    finally:
        crawler.close()
        if app:
            webserver.close(app)
        if config.read('CarbonStats'):
            timer.close()
        # apparently this is needed for full aiohttp cleanup
        loop.stop()
        loop.run_forever()
        loop.close()


if __name__ == '__main__':
    main()
    exit(stats.exitstatus)
