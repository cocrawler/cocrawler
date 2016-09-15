'''
Parse links in html and css pages.

XXX also need a gumbocy version
'''

import re

import stats

def find_html_links(html, url=None):
    '''
    Find the outgoing links and embeds in html

    On a 3.4ghz x86 core, this takes 20 milliseconds per megabyte
    '''
    with stats.record_burn('find_html_links re', url=url):
        ret = set(re.findall(r'''\s(?:href|src)=['"]?([^\s'"<>]+)''', html, re.I))
    return ret

def find_html_links_and_embeds(html, url=None):
    '''
    Find links in html, divided among links and embeds.
    More expensive than just getting unclassified links - 38 milliseconds/megabyte @ 3.4 ghz x86
    '''
    with stats.record_burn('find_html_links_and_embeds re', url=url):
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
    embeds = embeds_head.union(embeds_body)

    return links_body, embeds

def find_css_links(css, url=None):
    '''
    Finds the links embedded in css files
    '''

    with stats.record_burn('find_css_links re', url=url):
        ret = set(re.findall(r'''\surl\(\s?['"]?([^\s'"<>()]+)''', css, re.I))

    return ret
