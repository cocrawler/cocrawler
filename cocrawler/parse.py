'''
Parse links in html and css pages.

XXX also need a gumbocy alternative
'''

import re

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

def find_css_links(css):
    '''
    Finds the links embedded in css files
    '''
    stats.begin_cpu_burn('find_css_links re')
    ret = set(re.findall(r'''\surl\(\s?['"]?([^\s'"<>()]+)''', css, re.I))
    stats.end_cpu_burn('find_css_links re')
    return ret
