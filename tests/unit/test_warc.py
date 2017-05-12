import sys
import pytest

import cocrawler.warc as warc

try:
    from multidict import CIMultiDict, MultiDict
except ImportError:
    pass


def test_headers_to_str_headers():
    result = [('foo', 'bar'), ('baz', 'barf')]  # must have 2 pairs due to...
    rresult = result.copy().reverse()  # python < 3.5 doesn't have ordered dicts

    assert warc.headers_to_str_headers(result) == result

    header_dict = {'foo': b'bar', b'baz': 'barf'}
    ret = warc.headers_to_str_headers(header_dict)
    assert ret == result or ret == rresult

    aiohttp_raw_headers = ((b'foo', b'bar'), (b'baz', b'barf'))
    assert warc.headers_to_str_headers(aiohttp_raw_headers) == result

    aiohttp_headers = MultiDict(foo='bar', baz=b'barf')
    ret = warc.headers_to_str_headers(aiohttp_headers)
    assert ret == result or ret == rresult


@pytest.mark.skipif('multidict' not in sys.modules, reason='requires multidict be installed')
def test_multidict_headers_to_str_headers():
    # This case-insensitive thingie titlecases the key
    aiohttp_headers = CIMultiDict(foo='bar', baz=b'barf')
    titlecase_result = [('Foo', 'bar'), ('Baz', 'barf')]
    rtitlecase_result = titlecase_result.copy().reverse()

    ret = warc.headers_to_str_headers(aiohttp_headers)
    assert ret == titlecase_result or ret == rtitlecase_result
