'''
Misc unit tests, run first in Makefile.
'''

import unittest
import sys
import magic

class TestPythonVersion(unittest.TestCase):
    def test_python_version(self):
        self.assertGreaterEqual(sys.version_info, (3, 5), msg='Python 3.5+ needed for async def syntax')

    def test_magic(self):
        '''
        The python filemagic package requires the OS libmagic package,
        so let's test it to be sure nothing's missing
        '''
        with magic.Magic(flags=magic.MAGIC_MIME_TYPE) as m:
            pdf_string = '%PDF-1.3\n'
            self.assertEqual(m.id_buffer(pdf_string), 'application/pdf')
            html_string = '<html>\n'
            self.assertEqual(m.id_buffer(html_string), 'text/html')

if __name__ == '__main__':
    unittest.main()
