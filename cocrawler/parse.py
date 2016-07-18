import re
import unittest

import stats

'''
Parse links in html and css pages.

XXX also need a gumbocy alternative
'''

def find_html_links(html):
    '''
    Find the outgoing links in html
    '''
    # TODO: can I make a rule about embeds vs links?

    stats.begin_cpu_burn('find_html_links re')
    ret = set(re.findall(r'''\s(?:href|src)=['"]?([^\s'"<>]+)''', html, re.I))
    stats.end_cpu_burn('find_html_links re')
    return ret

test_html = '''
<html><header><title>Foo</title></header>
<a href="foo1.html"></a>
<a
href=foo2.htm></a>
<a
 href="foo3.html "></a>
<img src=foo.gif />
<body>
</body>
'''

def find_css_links(css):
    '''
    Finds the links embedded in css files
    '''
    stats.begin_cpu_burn('find_css_links re')
    ret = set(re.findall(r'''\surl\(\s?['"]?([^\s'"<>()]+)''', css, re.I))
    stats.end_cpu_burn('find_css_links re')
    return ret

test_css = '''
@import url('foo1.css')
url(images/foo2.png)
url( images/foo3.png )
'''

class TestParse(unittest.TestCase):
    def test_html_parse(self):
        links = find_html_links(test_html)
        self.assertEqual(len(links), 4)
        self.assertTrue('foo3.html' in links) # space?
        self.assertTrue('foo.gif' in links) # space?

    def test_css_parse(self):
        links = find_css_links(test_css)
        self.assertEqual(len(links), 3)
        self.assertTrue('images/foo3.png' in links) # space?

if __name__ == '__main__':
    unittest.main()
