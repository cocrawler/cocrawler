'''
Test cocrawler's warc shim

BUGS

Content-Type: text/dns
Content-Length: 0

how to insert my own digest? WARC-Payload-Digest

TODO

# check that WARC-Concurrent-To: in request is set to = WARC-Record-ID of response
# check that WARC-Warcinfo-ID is set to the WARC-Record-ID of the initial warcinfo in this file
# - heretrix does this according to example in warc standard, warcio's example does not
# - this is optional according to the standard, if the warcinfo record is at the top of the file

run through some kind of warc validation

freeze canonical output

'''

import hashlib
from collections import namedtuple

from cocrawler.warc import CCWARCWriter
from cocrawler.urls import URL

serials = {}


def get_serial(name):
    serial = serials.get(name, 0) + 1
    serials[name] = serial
    return '{:05}'.format(serial)


prefix = 'CC-TEST-01'
subprefix = 'FOO'
max_size = 10000

main = CCWARCWriter(prefix, max_size, get_serial=get_serial)
sub = CCWARCWriter(prefix, 1000, subprefix=subprefix, gzip=False, get_serial=get_serial)

main.create_default_info('1.0', '0.99', '127.0.0.1', description='desc', creator='test', operator='alice')
sub.create_default_info('1.0', '0.99', '127.0.0.1')

#main.write_robots('cocrawler.com/robots.txt', 'UserAgent: *\n'

#FakeDNSResponse = namedtuple('FakeDNSResponse', ['host', 'ttl'])

#fake_dns_result = [FakeDNSResponse('172.217.6.78', 10),
#                   FakeDNSResponse('172.217.6.78', 10),
#                   FakeDNSResponse('172.217.6.78', 10)]

fake_dns_result = [{'host': '172.217.6.78'},
                   {'host': '172.217.6.78'},
                   {'host': '172.217.6.78'}]

main.write_dns(fake_dns_result, 10, URL('http://google.com'))

fake_url = 'https://www.google.com/'
fake_req_headers = [('Host', 'www.google.com')]
fake_resp_headers = [(b'Content-Type', b'text/html; charset=UTF-8')]
fake_payload = b'<html><body>Hello, world!</body></html>'

digest = hashlib.sha1(fake_payload).hexdigest()
# to make sure that warcio is actually using our digest, multilate it
# this means we can't use a warc checker!
fake_digest = 'sha1:FAKE_DIGEST'

main.write_request_response_pair(fake_url, fake_req_headers, fake_resp_headers,
                                 False, fake_payload, digest=fake_digest)

# max size is set to 1000 for sub, make a payload that overflows it
fake_payload = ('x' * 80 + '\n') * 13
fake_payload = fake_payload.encode('utf-8')
digest = hashlib.sha1(fake_payload).hexdigest()

sub.write_request_response_pair(fake_url, fake_req_headers, fake_resp_headers,
                                False, fake_payload, digest=digest)
sub.write_request_response_pair(fake_url, fake_req_headers, fake_resp_headers,
                                False, fake_payload, digest=digest)

# XXX test of WARC-Truncate
