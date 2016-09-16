import parse

test_html = '''
<html>
<head><title>Foo</title><link href='link.html'></link></head>
<body>
<a href="foo1.html"></a>
<a
href=foo2.htm></a>
<a
 href="foo3.html "></a>
<img src=foo.gif />
</body>
'''

test_html_no_body = '''
<html>
<head><title>Foo</title><link href='link.html'></link></head>
<a href="foo1.html"></a>
<a
href=foo2.htm></a>
<a
 href="foo3.html "></a>
<img src=foo.gif />
'''

test_html_no_head = '''
<html>
<body>
<a href="foo1.html"></a>
<a
href=foo2.htm></a>
<a
 href="foo3.html "></a>
<img src=foo.gif />
</body>
'''

test_html_no_nothing = '''
<a href="foo1.html"></a>
<a
href=foo2.htm></a>
<a
 href="foo3.html "></a>
<img src=foo.gif />
'''

def test_html_parse():
    links, embeds = parse.find_html_links(test_html)
    assert len(links) == 5
    assert len(embeds) == 0
    assert 'foo3.html' in links # space?
    assert 'foo.gif' in links # space?

    links, embeds = parse.find_html_links_and_embeds(test_html)
    assert len(links) == 3
    assert len(embeds) == 2

    links, embeds = parse.find_html_links_and_embeds(test_html_no_body)
    assert len(links) == 3
    assert len(embeds) == 2

    links, embeds = parse.find_html_links_and_embeds(test_html_no_head)
    assert len(links) == 3
    assert len(embeds) == 1

    links, embeds = parse.find_html_links_and_embeds(test_html_no_nothing)
    assert len(links) == 3
    assert len(embeds) == 1

test_css = '''
@import url('foo1.css')
url(images/foo2.png)
url( images/foo3.png )
'''

def test_css_parse():
    links, embeds = parse.find_css_links(test_css)
    assert len(links) == 3
    assert len(embeds) == 0
    assert 'images/foo3.png' in links # space?
