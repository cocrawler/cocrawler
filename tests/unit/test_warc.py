import sys
import pytest
from collections import Counter

import cocrawler.warc as warc

try:
    from multidict import CIMultiDict, MultiDict
except ImportError:
    pass


def test_headers_to_str_headers():
    result = [('foo', 'bar'), ('baz', 'barf')]  # must have 2 pairs due to...
    ret = warc.headers_to_str_headers(result)
    assert Counter(ret) == Counter(result)

    header_dict = {'foo': b'bar', b'baz': 'barf'}
    ret = warc.headers_to_str_headers(header_dict)
    assert Counter(ret) == Counter(result)

    aiohttp_raw_headers = ((b'foo', b'bar'), (b'baz', b'barf'))
    ret = warc.headers_to_str_headers(aiohttp_raw_headers)
    assert Counter(ret) == Counter(result)

    aiohttp_headers = MultiDict(foo='bar', baz=b'barf')
    ret = warc.headers_to_str_headers(aiohttp_headers)
    assert Counter(ret) == Counter(result)


@pytest.mark.skipif('multidict' not in sys.modules, reason='requires multidict be installed')
def test_multidict_headers_to_str_headers():
    result = [('foo', 'bar'), ('baz', 'barf')]

    aiohttp_headers = MultiDict(foo='bar', baz=b'barf')
    ret = warc.headers_to_str_headers(aiohttp_headers)
    assert Counter(ret) == Counter(result)

    # This case-insensitive thingie titlecases the key, (sometimes ?!)

    titlecase_result = [('Foo', 'bar'), ('Baz', 'barf')]
    aiohttp_headers = CIMultiDict(foo='bar', baz=b'barf')

    ret = warc.headers_to_str_headers(aiohttp_headers)
    assert Counter(ret) == Counter(titlecase_result) or Counter(ret) == Counter(result)
