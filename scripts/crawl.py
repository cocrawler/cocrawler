#!/usr/bin/env python

'''
CoCrawler web crawler, main program
'''
import sys
import resource

import argparse
import asyncio
import logging

import cocrawler
import cocrawler.conf as conf
import cocrawler.stats as stats
import cocrawler.timer as timer
import cocrawler.webserver as webserver

ARGS = argparse.ArgumentParser(description='CoCrawler web crawler')
ARGS.add_argument('--config', action='append')
ARGS.add_argument('--configfile', action='store')
ARGS.add_argument('--no-confighome', action='store_true')
ARGS.add_argument('--no-test', action='store_true')
ARGS.add_argument('--printdefault', action='store_true')
ARGS.add_argument('--loglevel', action='store', type=int, default=2)
ARGS.add_argument('--load', action='store')


def limit_resources(config):
    _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))

    _, hard = resource.getrlimit(resource.RLIMIT_AS)  # RLIMIT_VMEM does not exist?!
    resource.setrlimit(resource.RLIMIT_AS, (16 * 1024 * 1024 * 1024, hard))  # XXX config


def main():
    '''
    Main program: parse args, read config, set up event loop, run the crawler.
    '''

    args = ARGS.parse_args()

    if args.printdefault:
        conf.print_default()
        sys.exit(1)

    levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
    logging.basicConfig(level=levels[min(args.loglevel, len(levels)-1)])

    config = conf.config(args.configfile, args.config, confighome=not args.no_confighome)
    limit_resources(config)

    kwargs = {}
    if args.load:
        kwargs['load'] = args.load
    if args.no_test:
        kwargs['no_test'] = True

    loop = asyncio.get_event_loop()
    crawler = cocrawler.Crawler(loop, config, **kwargs)

    if config.get('CarbonStats'):
        timer.start_carbon(loop, config)

    if config['REST']:
        app = webserver.make_app(loop, config)
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
        if config.get('CarbonStats'):
            timer.close()
        # apparently this is needed for full aiohttp cleanup
        loop.stop()
        loop.run_forever()
        loop.close()

if __name__ == '__main__':
    main()
    exit(stats.exitstatus)
