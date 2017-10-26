'''
Eventually we hope to use a robost html parser like gumbo, but for now
we're using BeautifulSoup+lxml in some circumstances. If we are, we
have to pay attention to this closed tag parsing bug, and our workaround.
'''

import pytest
from bs4 import BeautifulSoup

import cocrawler.parse as parse

html = '<body><a href="an href">anchortext</a></body></html>'


@pytest.mark.xfail(reason='close-tag FAIL', strict=True)
def test_lxml_close():
    defective_html = '</head>' + html
    body_soup = BeautifulSoup(defective_html, 'lxml')
    links, embeds = parse.find_body_links_soup(body_soup)
    assert len(links) == 1
    assert len(embeds) == 0


def test_lxml():
    body_soup = BeautifulSoup(html, 'lxml')
    links, embeds = parse.find_body_links_soup(body_soup)
    assert len(links) == 1
    assert len(embeds) == 0

    fixedup_html = '<html></head>' + html
    body_soup = BeautifulSoup(fixedup_html, 'lxml')
    links, embeds = parse.find_body_links_soup(body_soup)
    assert len(links) == 1
    assert len(embeds) == 0
