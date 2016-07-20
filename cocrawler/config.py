import logging
import yaml

LOGGER = logging.getLogger(__name__)

'''
default_yaml exists to both set defaults and to document all
possible configuration variables.
'''

default_yaml = '''
Seeds:
#  Hosts:
#  - http://xkcd.com/
#  File: seed_list.txt

Crawl:
  DepthLimit: 3
  MaxTries: 4
  MaxWorkers: 10
#  MaxCrawledUrls: 11
#  UserAgent: cocrawler/0.01

Robots:
  MaxTries: 4
  RobotsCacheSize: 1000
  RobotsCacheTimeout: 86400

Fetcher:
  Nameservers:
  - 8.8.8.8
  - 8.8.4.4

Plugins:
  Path:
  - ./plugins/generic
  url_allowed: SeedsHostname

Logging:
  LoggingLevel: 2
#  crawllog: crawllog.jsonl
#  robotslog: robotslog.jsonl

#Testing:
#  TestHostmap:
#    test.website: localhost:8080
#  StatsEQ:
#    fetch http code=200: 1000
#    URLs fetched: 1000
#    max urls found on a page: 3

'''

def print_default():
    print(default_yaml)

def merge_dicts(a, b):
    '''
    Merge 2-level dict b into a.
    Not very general purpose!
    '''
    c = a
    for k1 in b:
        for k2 in b[k1]:
            v = b[k1][k2]
            if k1 not in c or not c[k1]:
                c[k1] = {}
            if k2 not in c[k1]:
                c[k1][k2] = {}
            c[k1][k2] = v
    return c

def config(configfile, configlist):
    '''
    Return a config dict which is the sum of all the various configurations
    '''

    default = yaml.safe_load(default_yaml)

    config_from_file = {}
    if configfile:
        with open(configfile, 'r') as c:
            config_from_file = yaml.safe_load(c)

    combined = merge_dicts(default, config_from_file)

    if configlist:
        for c in configlist:
            # the syntax is... dangerous
            if ':' not in c:
                LOGGER.error('invalid config of %s', c)
                continue
            lhs, rhs = c.split(':', maxsplit=1)
            if '.' not in lhs:
                LOGGER.error('invalid config of %s', c)
                continue
            xpath = lhs.split('.')
            key = xpath.pop()
            try:
                temp = combined
                for x in xpath:
                    temp = combined[x]
                temp[key] = rhs
            except Exception as e:
                LOGGER.error('invalid config of %s, exception was %r', c, e)
                continue

    return combined
