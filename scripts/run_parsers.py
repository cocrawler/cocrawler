'''
Runs all of the available parsers over a tree of html

Accumulate cpu time
Compare counts of urls and embeds
'''

import sys
import os
import logging

import cocrawler.stats as stats
import cocrawler.parse as parse


def parse_all(name, string):
    head, body = parse.split_head_body(string)
    with stats.record_burn('split_head_body'):
        head, body = parse.split_head_body(string)

    links1, _ = parse.find_html_links_re(string, url=name)
    with stats.record_burn('find html links'):
        links1, _ = parse.find_html_links_re(string, url=name)
    links2, embeds2 = parse.find_body_links_re(body, url=name)
    with stats.record_burn('find body links re'):
        links2, embeds2 = parse.find_body_links_re(body, url=name)

    all2 = links2.union(embeds2)

    if len(links1) != len(all2):
        print('{} had different link counts of {} and {}'.format(name, len(links1), len(all2)))
        extra1 = links1.difference(all2)
        extra2 = all2.difference(links1)
        print('  extra in links:            {!r}'.format(extra1))
        print('  extra in links and embeds: {!r}'.format(extra2))
    return


LOGGER = logging.getLogger(__name__)

levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
logging.basicConfig(level=levels[3])

for d in sys.argv[1:]:
    for root, _, files in os.walk(d):
        for f in files:
            if f.endswith('.html') or f.endswith('.htm'):
                LOGGER.info(f)
                expanded = os.path.join(root, f)
                with open(expanded, 'r', errors='ignore') as fi:
                    parse_all(expanded, fi.read())

stats.report()
