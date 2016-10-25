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

def test_do_burner_work_html():
    test_html_bytes = test_html.encode(encoding='utf-8', errors='replace')
    links, embeds, sha1 = parse.do_burner_work_html(test_html, test_html_bytes)
    assert len(links) == 5
    assert len(embeds) == 0
    linkset = set(u.u for u in links)
    assert 'foo3.html' in linkset # space?
    assert 'foo.gif' in linkset # space?
    assert sha1 == 'sha1:8ea2d7e90c956118c451819330b875994f96f511'

def test_misc_parsers():
    links, embeds = parse.find_html_links_and_embeds(test_html)
    assert len(links) == 3
    assert len(embeds) == 2
    linkset = set(u.u for u in links)
    embedset = set(u.u for u in embeds)
    assert 'foo3.html' in linkset # space?
    assert 'foo.gif' in embedset # space?

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
    linkset = set(u.u for u in links)
    assert 'images/foo3.png' in linkset # space?
