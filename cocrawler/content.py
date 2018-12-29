'''
Charset and encoding-related code
'''
import logging
import zlib
import codecs
import brotli
import cgi

from . import stats

try:
    import cchardet as chardet
except ImportError:  # pragma: no cover
    import chardet

LOGGER = logging.getLogger(__name__)


def get_accept_encoding():
    return 'identity, deflate, gzip, br'


def decompress(body_bytes, content_encoding, url=None):
    content_encoding = content_encoding.lower()
    if content_encoding == 'deflate':
        try:
            return zlib.decompress(body_bytes, zlib.MAX_WBITS)  # expects header/checksum
        except Exception:
            try:
                # http://www.gzip.org/zlib/zlib_faq.html#faq38
                stats.stats_sum('content-encoding deflate fallback try', 1)
                return zlib.decompress(body_bytes, -zlib.MAX_WBITS)  # no header/checksum
            except Exception as e:
                LOGGER.debug('deflate fail for url %s: %s', url, str(e))
                stats.stats_sum('content-encoding deflate fail', 1)
                return body_bytes
    elif content_encoding == 'gzip' or content_encoding == 'x-gzip':
        try:
            return zlib.decompress(body_bytes, 16 + zlib.MAX_WBITS)
        except Exception as e:
            LOGGER.debug('gzip fail for url %s: %s', url, str(e))
            stats.stats_sum('content-encoding gzip fail', 1)
            return body_bytes
    elif content_encoding == 'br':
        try:
            return brotli.decompress(body_bytes)
        except Exception as e:
            LOGGER.debug('bz fail for url %s: %s', url, str(e))
            stats.stats_sum('content-encoding brotli fail', 1)
            return body_bytes
    else:
        # 'identity' is in the standard
        # also fairly common to have 'raw', 'none', or a charset
        return body_bytes


def parse_headers(resp_headers, json_log):
    content_type = resp_headers.get('content-type', '')
    # sometimes content_type comes back multiline. whack it with a wrench.
    content_type = content_type.replace('\r', '\n').partition('\n')[0].lower()
    content_type, options = cgi.parse_header(content_type)

    json_log['content_type'] = content_type
    stats.stats_sum('content-type=' + content_type, 1)
    if 'charset' in options:
        charset = options['charset'].lower()
        json_log['content_type_charset'] = charset
        stats.stats_sum('content-type-charset=' + charset, 1)
    else:
        charset = None
        stats.stats_sum('content-type-charset=' + 'not specified', 1)

    content_encoding = resp_headers.get('content-encoding', 'identity').lower()
    if content_encoding != 'identity':
        json_log['content_encoding'] = content_encoding
        stats.stats_sum('content-encoding=' + content_encoding, 1)

    transfer_encoding = resp_headers.get('transfer-encoding', 'identity').lower()
    if transfer_encoding != 'identity':
        json_log['transfer_encoding'] = transfer_encoding
        stats.stats_sum('transfer-encoding=' + transfer_encoding, 1)

    return content_type, content_encoding, charset


# because we're using the streaming interface we can't call resp.get_encoding()
# this is the same algo as aiohttp
# using the name 'charset' because 'encoding' is a python concept, not http
# TODO:
# https://encoding.spec.whatwg.org/#names-and-labels says we should map
#   iso9958-1 to windows-1252 since 1252 a supserset, and a common problem on the actual web
# https://encoding.spec.whatwg.org/encodings.json
def my_get_charset(charset, body_bytes):
    detect = chardet.detect(body_bytes)
    if detect['encoding']:
        detect['encoding'] = detect['encoding'].lower()
    if detect['confidence']:
        detect['confidence'] = '{:.2f}'.format(detect['confidence'])

    for cset in (charset, detect['encoding'], 'utf-8'):
        if cset:
            try:
                codecs.lookup(cset)
                break
            except LookupError:
                pass
    else:
        cset = None

    return cset, detect


def my_decode(body_bytes, charset, detect):
    for cset in charset, detect['encoding']:
        if not cset:
            # encoding or detect may be None
            continue
        try:
            body = body_bytes.decode(encoding=cset)
            break
        except UnicodeDecodeError:
            # if we truncated the body, we could have caused the error:
            # UnicodeDecodeError: 'utf-8' codec can't decode byte 0xd9 in position 15: unexpected end of data
            # or encoding could be wrong, or the page could be defective
            pass
        except LookupError as e:
            if e.args[0].startswith('unknown encoding: '):  # e.g. viscii
                stats.stats_sum('content-encoding unknown: '+cset, 1)
                pass
    else:
        body = body_bytes.decode(encoding='utf-8', errors='replace')
        cset = 'utf-8 replace'
    return body, cset
