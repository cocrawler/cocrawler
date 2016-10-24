#!/usr/bin/env python

'''
CoCrawler web crawler, main program
'''
import sys
import resource

import argparse
import asyncio
import logging
from aiohttp import web

import config
import cocrawler
import stats
import timer
import webserver

ARGS = argparse.ArgumentParser(description='CoCrawler web crawler')
ARGS.add_argument('--config', action='append')
ARGS.add_argument('--configfile', action='store')
ARGS.add_argument('--no-confighome', action='store_true')
ARGS.add_argument('--no-test', action='store_true')
ARGS.add_argument('--printdefault', action='store_true')
ARGS.add_argument('--loglevel', action='store', type=int, default=2)
ARGS.add_argument('--load', action='store')

def limit_resources(config):
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))

    soft, hard = resource.getrlimit(resource.RLIMIT_AS) # RLIMIT_VMEM does not exist?!
    resource.setrlimit(resource.RLIMIT_AS, (16 * 1024 * 1024 * 1024, hard)) # XXX config

def main():
    '''
    Main program: parse args, read config, set up event loop, run the crawler.
    '''

    args = ARGS.parse_args()

    if args.printdefault:
        config.print_default()
        sys.exit(1)

    levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
    logging.basicConfig(level=levels[min(args.loglevel, len(levels)-1)])

    conf = config.config(args.configfile, args.config, confighome=not args.no_confighome)
    limit_resources(conf)

    kwargs = {}
    if args.load:
        kwargs['load'] = args.load
    if args.no_test:
        kwargs['no_test'] = True

    loop = asyncio.get_event_loop()
    crawler = cocrawler.Crawler(loop, conf, **kwargs)

    if conf.get('CarbonStats'):
        timer.start_carbon(loop, conf)

    if conf['REST']:
        app = webserver.make_app(loop, conf)
    else:
        app = None

    try:
        loop.run_until_complete(crawler.crawl())
    except KeyboardInterrupt:
        sys.stderr.flush()
        print('\nInterrupt. Exiting cleanly.\n')
        crawler.cancel_workers()
    finally:
        crawler.close()
        if app:
            webserver.close(app)
        if conf.get('CarbonStats'):
            timer.close()
        # apparently this is needed for full aiohttp cleanup
        loop.stop()
        loop.run_forever()
        loop.close()

if __name__ == '__main__':
    main()
    exit(stats.exitstatus)
