import cocrawler.warc as warc

from multidict import CIMultiDict, MultiDict


def test_headers_to_str_headers():
    result = [('foo', 'bar'), ('baz', 'barf')]
    result2 = [('baz', 'barf'), ('foo', 'bar')]  # because dict order is not in python 3.5
    assert warc.headers_to_str_headers(result) == result

    header_dict = {'foo': b'bar', b'baz': 'barf'}
    ret = warc.headers_to_str_headers(header_dict)
    assert ret == result or ret == result2

    aiohttp_raw_headers = ((b'foo', b'bar'), (b'baz', b'barf'))
    assert warc.headers_to_str_headers(aiohttp_raw_headers) == result

    aiohttp_headers = MultiDict(foo='bar', baz=b'barf')
    ret = warc.headers_to_str_headers(aiohttp_headers)
    assert ret == result or ret == result2

    # This case-insensitive thingie titlecases the key
    aiohttp_headers = CIMultiDict(foo='bar', baz=b'barf')
    titlecase_result = [('Foo', 'bar'), ('Baz', 'barf')]
    titlecase_result2 = [('Baz', 'barf'), ('Foo', 'bar')]
    ret = warc.headers_to_str_headers(aiohttp_headers)
    assert ret == titlecase_result or ret == titlecase_result2
