'''
Runs all of the available parsers over a tree of html

Accumulate cpu time
Compare counts of urls and embeds
'''

import sys
import os
import logging

from bs4 import BeautifulSoup

import cocrawler.stats as stats
import cocrawler.parse as parse


def parse_all(name, string):
    all_links = []

    # warmup

    head, body = parse.split_head_body(string)

    links, embeds = parse.find_html_links_re(string)  # embeds is empty here by design
    links, embeds = parse.find_body_links_re(body)

    head_soup = BeautifulSoup(head, 'lxml')
    body_soup = BeautifulSoup(body, 'lxml')
    links, embeds = parse.find_head_links_soup(head_soup)
    links, embeds = parse.find_body_links_soup(body_soup)

    # measurement

    with stats.record_burn('split_head_body', url=name):
        head, body = parse.split_head_body(string)

    with stats.record_burn('find_html_links_re', url=name):
        links, embeds = parse.find_html_links_re(string)  # embeds is empty here by design
        all_links.append(links.union(embeds))

    with stats.record_burn('head_soup', url=name):
        head_soup = BeautifulSoup(head, 'lxml')
    with stats.record_burn('find_head_links_soup', url=name):
        head_links, head_embeds = parse.find_head_links_soup(head_soup)

    with stats.record_burn('find_body_links_re', url=name):
        links, embeds = parse.find_body_links_re(body)
        all_links.append(links.union(embeds).union(head_links).union(head_embeds))

    with stats.record_burn('body_soup', url=name):
        body_soup = BeautifulSoup(body, 'lxml')
    with stats.record_burn('find_body_links_soup', url=name):
        links, embeds = parse.find_body_links_soup(body_soup)
        all_links.append(links.union(embeds).union(head_links).union(head_embeds))

    # evaluation
    return

    if len(all1) != len(all2):
        print('{} had different link counts of {} and {}'.format(name, len(all1), len(all2)))
        extra1 = all1.difference(all2)
        extra2 = all2.difference(all1)
        print('  extra in links:            {!r}'.format(extra1))
        print('  extra in links and embeds: {!r}'.format(extra2))
    return


LOGGER = logging.getLogger(__name__)

levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
logging.basicConfig(level=levels[3])

for d in sys.argv[1:]:
    if os.path.isfile(d):
        with open(d, 'r', errors='ignore') as fi:
            parse_all(d, fi.read())
        continue
    for root, _, files in os.walk(d):
        for f in files:
            if f.endswith('.html') or f.endswith('.htm'):
                expanded = os.path.join(root, f)
                with open(expanded, 'r', errors='ignore') as fi:
                    parse_all(expanded, fi.read())

stats.report()
