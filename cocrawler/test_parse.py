import pytest

import parse

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

def test_html_parse():
    links = parse.find_html_links(test_html)
    assert len(links) == 5
    assert 'foo3.html' in links # space?
    assert 'foo.gif' in links # space?

    links, embeds = parse.find_html_links_and_embeds(test_html)
    assert len(links) == 3
    assert len(embeds) == 2

test_css = '''
@import url('foo1.css')
url(images/foo2.png)
url( images/foo3.png )
'''

def test_css_parse():
    links = parse.find_css_links(test_css)
    assert len(links) == 3
    assert 'images/foo3.png' in links # space?

