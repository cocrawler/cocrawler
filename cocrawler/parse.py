'''
Parse links in html and css pages.

XXX also need a gumbocy version
'''

import re
import time

import stats

def find_html_links(html):
    '''
    Find the outgoing links and embeds in html
    '''

    start = time.clock()
    ret = set(re.findall(r'''\s(?:href|src)=['"]?([^\s'"<>]+)''', html, re.I))
    stats.record_cpu_burn('find_html_links re', start)
    return ret

def find_html_links_and_embeds(html):
    '''
    Find links in html, divided among links and embeds.
    More expensive than just getting unclassified links.
    '''

    start = time.clock()
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
    stats.record_cpu_burn('find_html_links_and_embeds re', start)

    embeds = embeds_head.union(embeds_body)

    return links_body, embeds

def find_css_links(css):
    '''
    Finds the links embedded in css files
    '''
    start = time.clock()
    ret = set(re.findall(r'''\surl\(\s?['"]?([^\s'"<>()]+)''', css, re.I))
    stats.record_cpu_burn('find_css_links re', start)
    return ret
