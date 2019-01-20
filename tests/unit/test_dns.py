'''
This test does talk to the network... that might be a little surprising
for something that purports to be a unit test.
'''

import pytest

import cocrawler.dns as dns
from cocrawler.urls import URL
import cocrawler.config as config


@pytest.mark.asyncio
async def test_prefetch():
    url = URL('http://example.com/')

    config.config(None, None)
    resolver = dns.get_resolver()

    iplist = await dns.prefetch(url, resolver)
    assert len(iplist) > 0
    iplist2 = await dns.prefetch(url, resolver)
    assert iplist == iplist2


def test_entry_to_ip_key():
    addrs = [{'host': '4.3.2.1'}, {'host': '8.8.8.8'}, {'host': '1.2.3.4'}]
    result = '1.2.3.4,4.3.2.1,8.8.8.8'
    assert dns.entry_to_ip_key([addrs, None]) == result
    assert dns.entry_to_ip_key(None) is None
