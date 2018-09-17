'''
Misc unit tests, run first in Makefile.
'''

import sys
import pytest
#import magic  # needs libmagic installed in the OS to work


def test_python_version():
    assert sys.version_info >= (3, 5), 'Python 3.5+ needed for async def syntax'


@pytest.mark.skip(reason='Not currently using magic anyway')
def test_magic():
    '''
    The python filemagic package requires the OS libmagic package,
    so let's test it to be sure nothing's missing
    '''
    with magic.Magic(flags=magic.MAGIC_MIME_TYPE) as m:
        pdf_string = '%PDF-1.3\n'
        assert m.id_buffer(pdf_string) == 'application/pdf'
        html_string = '<html>\n'
        assert m.id_buffer(html_string) == 'text/html'
