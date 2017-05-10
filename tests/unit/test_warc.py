import cocrawler.warc as warc

from multidict import CIMultiDict, MultiDict


def test_headers_to_str_headers():
    result = [('foo', 'bar'), ('baz', 'barf')]
    assert warc.headers_to_str_headers(result) == result

    header_dict = {'foo': b'bar', b'baz': 'barf'}
    assert warc.headers_to_str_headers(header_dict) == result

    aiohttp_raw_headers = ((b'foo', b'bar'), (b'baz', b'barf'))
    assert warc.headers_to_str_headers(aiohttp_raw_headers) == result

    aiohttp_headers = MultiDict(foo='bar', baz=b'barf')
    assert warc.headers_to_str_headers(aiohttp_headers) == result

    # This case-insensitive thingie titlecases the key
    aiohttp_headers = CIMultiDict(foo='bar', baz=b'barf')
    titlecase_result = [('Foo', 'bar'), ('Baz', 'barf')]
    assert warc.headers_to_str_headers(aiohttp_headers) == titlecase_result
