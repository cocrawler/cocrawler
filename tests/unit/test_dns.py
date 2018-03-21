'''
This test does talk to the network... that might be a little surprising
for something that purports to be a unit test.
'''

import socket
import logging

import aiohttp
import pytest

import cocrawler.dns as dns
from cocrawler.urls import URL
import aiodns.error

levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
logging.basicConfig(level=levels[3])

ns = ['8.8.8.8', '8.8.4.4']  # google


@pytest.mark.asyncio
@pytest.mark.skip(reason="This is testing dead code, needs to test the live code")
async def test_prefetch_dns():
    url = URL('http://google.com/')
    mock_url = None
    resolver = aiohttp.resolver.AsyncResolver(nameservers=ns)
    connector = aiohttp.connector.TCPConnector(resolver=resolver, family=socket.AF_INET)
    session = aiohttp.ClientSession(connector=connector)

    # whew

    iplist = await dns.prefetch_dns(url, mock_url, session)

    assert len(iplist) > 0
    session.close()

@pytest.mark.asyncio
@pytest.mark.skip(reason="This is testing dead code, needs to test the live code")
async def test_resolver():
    dns.setup_resolver(ns)

    iplist = await dns.query('google.com', 'A')
    assert len(iplist) > 0

    iplist = await dns.query('google.com', 'AAAA')
    assert len(iplist) > 0

    iplist = await dns.query('google.com', 'NS')
    assert len(iplist) > 0

    with pytest.raises(aiodns.error.DNSError):
        iplist = await dns.query('google.com', 'CNAME')
        assert iplist is None

    iplist = await dns.query('www.blogger.com', 'CNAME')
    assert len(iplist) > 0
