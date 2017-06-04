'''
Parses a webpage using Beautiful Soup and prints the hrefs
and anchortext. Useful for working out what a mature parser
does with screwed up html.
'''

import sys

from bs4 import BeautifulSoup


def parse_and_print(html):
    soup = BeautifulSoup(html, 'lxml')
    ahref = soup.find_all('a')
    for a in ahref:
        print(a)


for f in sys.argv[1:]:
    with open(f, 'r') as fi:
        parse_and_print(fi.read())
