'''
Benchmark the configured DNS service.

Verify that DNS more-or-less works.
'''

import argparse
import asyncio
import socket

import aiodns
import aiohttp

import cocrawler.conf as conf
import cocrawler.dns as dns

ARGS = argparse.ArgumentParser(description='CoCrawler dns benchmark')
ARGS.add_argument('--config', action='append')
ARGS.add_argument('--configfile', action='store')
ARGS.add_argument('--no-confighome', action='store_true')

args = ARGS.parse_args()

config = conf.config(args.configfile, args.config, confighome=not args.no_confighome)
max_workers = config['Crawl']['MaxWorkers']

ns = config['Fetcher'].get('Nameservers')
resolver = aiohttp.resolver.AsyncResolver(nameservers=ns)  # Can I pass rotate=True into this?
connector = aiohttp.connector.TCPConnector(resolver=resolver, family=socket.AF_INET)
session = aiohttp.ClientSession(connector=connector)

res = aiodns.DNSResolver(nameservers=ns, rotate=True)


async def work():
    iplist = await dns.prefetch_dns(url, mock_url, session)


loop = asyncio.get_event_loop()
workers = [asyncio.Task(work(), loop=loop) for _ in range(max_workers)]

# XXX INCOMPLETE
