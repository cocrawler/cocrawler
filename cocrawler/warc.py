import os
import socket
import logging
from collections import OrderedDict
from io import BytesIO

from . import config

from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter
from warcio.timeutils import timestamp_now

from . import stats

LOGGER = logging.getLogger(__name__)


valid_truncations = (('length', 'time', 'disconnect', 'unspecified'))


'''
best practices from http://www.netpreserve.org/sites/default/files/resources/WARC_Guidelines_v1.pdf

filenames: prefix-timestamp-serial-crawlhost.warc.gz
 encourage users to configure prefix different for each crawl
  example: prefix=BNF-CRAWL-003
 serial should be unique wrt prefix

Common Crawl has shifted to prefix-starttime-endtime-serial.warc.gz
 we can't do that unless we postprocess 

warcinfo at the start of every file: WARC-Filename in case of a rename, repeat crawl configuration info
have a WARC-Warcinfo-ID field for every record
logfiles in a warcinfo record (might have to segment if big)
final warc file: record with a manifest of all warcs created in the crawl
'''

'''
warcinfo content from heretrix:

software: Heritrix 1.12.0 http://crawler.archive.org
hostname: crawling017.archive.org
ip: 207.241.227.234
isPartOf: testcrawl-20050708 {{Dublin Core}}
description: testcrawl with WARC output {{Dublin Core}}}
operator: IA\\_Admin {why the \\_?} {{1.0 standard says should be contact info, name or name and email}}
http-header-user-agent:
 Mozilla/5.0 (compatible; heritrix/1.4.0 +http://crawler.archive.org) {{redundant with info in request record}}
format: WARC file version 1.0 {{Dublin Core}}
conformsTo:
 http://www.archive.org/documents/WarcFileFormat-1.0.html {{Dublin Core}}

warcinfo from warcio package example.warc:

WARC/1.0
WARC-Date: 2017-03-06T04:03:53Z
WARC-Record-ID: <urn:uuid:e9a0ee48-0221-11e7-adb1-0242ac120008>
WARC-Filename: temp-20170306040353.warc.gz
WARC-Type: warcinfo
Content-Type: application/warc-fields
Content-Length: 470

software: Webrecorder Platform v3.7
format: WARC File Format 1.0
creator: temp-MJFXHZ4S {{Dublin Core: person, organization, or service}}
isPartOf: Temporary%20Collection/Recording%20Session
json-metadata: {"created_at": 1488772924, "type": "recording", "updated_at": 1488773028, "title": "Recording Session", "size": 2865, "pages": [{"url": "http://example.com/", "title": "Example Domain", "timestamp": "20170306040348"}, {"url": "http://example.com/", "title": "Example Domain", "timestamp": "20170306040206"}]}
'''


class CCWARCWriter:
    def __init__(self, prefix, max_size, subprefix=None, gzip=True, get_serial=None):
        self.writer = None
        self.prefix = prefix
        self.subprefix = subprefix
        self.max_size = max_size
        self.gzip = gzip
        self.hostname = socket.gethostname()
        if get_serial is not None:
            self.external_get_serial = get_serial
        else:
            self.external_get_serial = None
            self.serial = 0

    def __del__(self):
        if self.writer is not None:
            self.f.close()

    def create_default_info(self, version, warcheader_version, ip, description=None, creator=None, operator=None):
        '''
        creator:  # person, organization, service
        operator:  # person, if creator is an organization
        isPartOf:  # name of the crawl
        '''
        info = OrderedDict()

        info['software'] = 'cocrawler/' + version + ' cocrawler_warcheader_version/' + warcheader_version
        info['hostname'] = self.hostname
        info['ip'] = ip
        if description:
            info['description'] = description
        if creator:
            info['creator'] = creator
        if operator:
            info['operator'] = operator
        info['isPartOf'] = self.prefix  # intentionally does not include subprefix
        info['format'] = 'WARC file version 1.0'
        self.info = info
        return info

    def open(self):
        filename = self.prefix
        if self.subprefix:
            filename += '-' + str(self.subprefix)  # don't let yaml leave this as an int
        serial = self.get_serial(filename)
        filename += '-' + serial + '-' + self.hostname + '.warc'
        if self.gzip:
            filename += '.gz'
        self.filename = filename
        self.f = open(filename, 'wb')
        self.writer = WARCWriter(self.f, gzip=self.gzip)
        record = self.writer.create_warcinfo_record(self.filename, self.info)
        self.warcinfo_id = record.rec_headers.get_header('WARC-Record-ID')
        self.writer.write_record(record)

    def get_serial(self, filename):
        if self.external_get_serial is not None:
            return self.external_get_serial(filename)
        self.serial += 1
        return '{:06}'.format(self.serial-1)

    def maybe_close(self):
        '''
        TODO: always close/reopen if subprefix is not None; to minimize open filehandles?
        '''
        fsize = os.fstat(self.f.fileno()).st_size
        if fsize > self.max_size:
            self.f.close()
            self.writer = None

    def write_dns(self, dns, ttl, url):
        # write it out even if empty
        # TODO: we filter the addresses early, should we warc the unfiltered dns repsonse?

        # the response object doesn't contain the query type 'A' or 'AAAA'
        # but it has family=2 AF_INET (ipv4) and flags=4 AI_NUMERICHOST -- that's 'A'
        kind = 'A'  # fixme IPV6

        ttl = int(ttl)
        host = url.hostname

        if self.writer is None:
            self.open()

        payload = timestamp_now() + '\r\n'

        for r in dns:
            try:
                payload += '\t'.join((host+'.', str(ttl), 'IN', kind, r['host'])) + '\r\n'
            except Exception as e:
                LOGGER.info('problem converting dns reply for warcing', host, r, e)
                pass
        payload = payload.encode('utf-8')

        warc_headers_dict = OrderedDict()
        warc_headers_dict['WARC-Warcinfo-ID'] = self.warcinfo_id

        record = self.writer.create_warc_record('dns:'+host, 'resource',
                                                warc_content_type='text/dns',
                                                payload=BytesIO(payload),
                                                length=len(payload),
                                                warc_headers_dict=warc_headers_dict)

        self.writer.write_record(record)
        LOGGER.debug('wrote warc dns response record%s for host %s', p(self.prefix), host)
        stats.stats_sum('warc dns'+p(self.prefix), 1)

    def _fake_resp_headers(self, resp_headers, body_len, decompressed=False):
        prefix = b'X-Crawler-'
        ret = []
        for h, v in resp_headers:
            hl = h.lower()
            if hl == b'content-length':
                if not(v.isdigit() and int(v) == body_len):
                    ret.append((prefix+h, v))
                    ret.append((b'Content-Length', str(body_len)))
            elif hl == b'content-encoding':
                if decompressed:
                    ret.append((prefix+h, v))
                else:
                    ret.append((h, v))
            elif hl == b'transfer-encoding':
                if v.lower() == b'chunked':
                    # aiohttp always undoes chunking
                    ret.append((prefix+h, v))
                else:
                    ret.append((h, v))
            else:
                ret.append((h, v))
        return ret

    def write_request_response_pair(self, url, ip, req_headers, resp_headers, is_truncated, payload, digest=None, decompressed=False):
        if self.writer is None:
            self.open()

        req_http_headers = StatusAndHeaders('GET / HTTP/1.1', req_headers)

        warc_headers_dict = OrderedDict()
        warc_headers_dict['WARC-Warcinfo-ID'] = self.warcinfo_id
        request = self.writer.create_warc_record('http://example.com/', 'request',
                                                 warc_headers_dict=warc_headers_dict,
                                                 http_headers=req_http_headers)

        fake_resp_headers = self._fake_resp_headers(resp_headers, len(payload), decompressed=decompressed)
        resp_http_headers = StatusAndHeaders('200 OK', fake_resp_headers, protocol='HTTP/1.1')

        warc_headers_dict = OrderedDict()
        warc_headers_dict['WARC-Warcinfo-ID'] = self.warcinfo_id
        if ip is not None:
            if not isinstance(ip, str):
                ip = ip[0]
            warc_headers_dict['WARC-IP-Address'] = ip
        if digest is not None:
            warc_headers_dict['WARC-Payload-Digest'] = digest
        if is_truncated:
            if is_truncated in valid_truncations:
                warc_headers_dict['WARC-Truncated'] = is_truncated
            else:
                LOGGER.error('Invalid is_truncation of '+is_truncated)
                warc_headers_dict['WARC-Truncated'] = 'unspecified'

        response = self.writer.create_warc_record(url, 'response',
                                                  payload=BytesIO(payload),
                                                  length=len(payload),
                                                  warc_headers_dict=warc_headers_dict,
                                                  http_headers=resp_http_headers)

        self.writer.write_request_response_pair(request, response)
        self.maybe_close()
        LOGGER.debug('wrote warc request-response pair%s for url %s', p(self.prefix), url)
        stats.stats_sum('warc r/r'+p(self.prefix), 1)


def p(prefix):
    if prefix:
        return ' (prefix '+prefix+')'
    else:
        return ''


def setup(version, warcheader_version, local_addr):
    warcall = config.read('WARC', 'WARCAll')
    if warcall is not None and warcall:
        max_size = config.read('WARC', 'WARCMaxSize')
        prefix = config.read('WARC', 'WARCPrefix')
        subprefix = config.read('WARC', 'WARCSubPrefix')
        description = config.read('WARC', 'WARCDescription')
        creator = config.read('WARC', 'WARCCreator')
        operator = config.read('WARC', 'WARCOperator')
        warcwriter = CCWARCWriter(prefix, max_size, subprefix=subprefix)  # XXX get_serial lacks a default
        warcwriter.create_default_info(version, warcheader_version, local_addr,
                                       description=description, creator=creator, operator=operator)
    else:
        warcwriter = None
    return warcwriter
