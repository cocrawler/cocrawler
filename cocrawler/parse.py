'''
Parse links in html and css pages.

XXX also need a gumbocy alternative
'''

import re
import unittest

import stats

def find_html_links(html):
    '''
    Find the outgoing links in html
    '''

    stats.begin_cpu_burn('find_html_links re')
    ret = set(re.findall(r'''\s(?:href|src)=['"]?([^\s'"<>]+)''', html, re.I))
    stats.end_cpu_burn('find_html_links re')
    return ret

def find_html_links_and_embeds(html):
    '''
    Find links in html, divided among links and embeds. More expensive
    than just getting unclassified links
    '''

    stats.begin_cpu_burn('find_html_links_and_embeds re')
    try:
        head, body = html.split('<body>', maxsplit=1)
    except ValueError:
        try:
            head, body = html.split('</head>', maxsplit=1)
        except ValueError:
            head = ''
            body = html
    embeds_head = set(re.findall(r'''\s(?:href|src)=['"]?([^\s'"<>]+)''', head, re.I))
    embeds_body = set(re.findall(r'''\ssrc=['"]?([^\s'"<>]+)''', body, re.I))
    links_body = set(re.findall(r'''\shref=['"]?([^\s'"<>]+)''', body, re.I))
    stats.end_cpu_burn('find_html_links_and_embeds re')

    return links_body, embeds_head.union(embeds_body)

test_html = '''
<html>
<header><title>Foo</title><link href='link.html'></link></header>
<body>
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
        self.assertEqual(len(links), 5)
        self.assertTrue('foo3.html' in links) # space?
        self.assertTrue('foo.gif' in links) # space?
        links, embeds = find_html_links_and_embeds(test_html)
        self.assertEqual(len(links), 3)
        self.assertEqual(len(embeds), 2)

    def test_css_parse(self):
        links = find_css_links(test_css)
        self.assertEqual(len(links), 3)
        self.assertTrue('images/foo3.png' in links) # space?

if __name__ == '__main__':
    unittest.main()
