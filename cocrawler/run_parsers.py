'''
Runs all of the available parsers over a tree of html

Accumulate cpu time
Compare counts of urls and embeds
'''

import sys
import os
import logging

import stats
import parse

def parse_all(name, string):
    links1, _ = parse.find_html_links(string, url=name)
    links2, embeds2 = parse.find_html_links_and_embeds(string, url=name)

    all2 = links2.union(embeds2)

    if len(links1) != len(all2):
        print('{} had different link counts of {} and {}'.format(name, len(links1), len(all2)))
        extra1 = links1.difference(all2)
        extra2 = all2.difference(links1)
        print('  extra in links:            {!r}'.format(extra1))
        print('  extra in links and embeds: {!r}'.format(extra2))
    return

for d in sys.argv[1:]:
    for root, _, files in os.walk(d):
        for name in files:
            if name.endswith('.html') or name.endswith('.htm'):
                expanded = os.path.join(root, name)
                with open(expanded, 'r', errors='ignore') as f:
                    parse_all(expanded, f.read())

levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
logging.basicConfig(level=levels[3])
stats.report()
