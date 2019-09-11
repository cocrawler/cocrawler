import urllib
import logging

from . import config
from . import stats
from .urls import URL
from . import url_allowed

LOGGER = logging.getLogger(__name__)

POLICY = None
valid_policies = set(('None', 'www-then-non-www'))


def sanatize(line, dedup):
    if '#' in line:
        line, _ = line.split('#', 1)
    line = line.strip()
    if line == '':
        return None, None
    u = special_seed_handling(line)
    if u is None:
        return None, None
    if u in dedup:
        return None, None
    dedup.add(u)
    return line, u


def expand_seeds_config(crawler):
    urls = []
    seeds = config.read('Seeds')

    if seeds is None:
        return

    global POLICY
    POLICY = config.read('Seeds', 'Policy')
    if POLICY not in valid_policies:
        raise ValueError('config Seeds Policy is not valid: '+POLICY)
    else:
        LOGGER.info('configuring a seeds policy of %s', POLICY)

    if seeds.get('Hosts', []):
        for h in seeds['Hosts']:
            u = special_seed_handling(h)
            if u is not None:
                urls.append((h, u))

    if seeds.get('CrawledHosts', []):
        for h in seeds['CrawledHosts']:
            u = special_seed_handling(h)
            if u is not None:
                crawler.datalayer.add_seen(URL(u))

    seed_files = seeds.get('Files', [])
    dedup = set()
    if seed_files:
        if not isinstance(seed_files, list):
            seed_files = [seed_files]
        for name in seed_files:
            name = str(name)  # yaml leaves filenames like '1000' as ints
            LOGGER.info('Loading seeds from file %s', name)
            with open(name, 'r') as f:
                for line in f:
                    seed_host, u = sanatize(line, dedup)
                    if seed_host:
                        urls.append((seed_host, u))

    final_urls = []
    for seed_host, u in urls:
        url = URL(u)
        if POLICY == 'www-then-non-www':
            # url already has a scheme, may or may not have www
            if url.hostname == url.hostname_without_www:
                if u.startswith('http://'):
                    second = u.replace('http://', 'http://www.', 1)
                elif u.startswith('https://'):
                    second = u.replace('https://', 'http://www.', 1)  # second chance is always http
                else:
                    LOGGER.error('skipping invalid seed: '+seed_host+' '+u)
            else:
                second = u.replace('://www.', '://', 1)
                if second == u:
                    #raise ValueError('invalid seed 2: '+seed_host+' '+u)
                    print('invalid seed 2: '+seed_host+' '+u)  # example: http://www3.nhk.or.jp
                    second = ''
        else:
            second = ''
        final_urls.append((seed_host, url, second))

    crawled_files = seeds.get('CrawledFiles')
    if crawled_files:
        if not isinstance(crawled_files, list):
            crawled_files = [crawled_files]
        for name in crawled_files:
            name = str(name)
            LOGGER.info('loading crawled urls from file %s', name)
            dedup = set()
            with open(name, 'r') as f:
                for line in f:
                    seed_host, u = sanatize(line, dedup)
                    if seed_host:
                        crawler.datalayer.add_seen(URL(u))

    return seed_some_urls(final_urls, crawler)


def seed_some_urls(urls, crawler, skip_crawled=False):
    freeseedredirs = config.read('Seeds', 'FreeSeedRedirs')
    retries_left = config.read('Seeds', 'SeedRetries') or config.read('Crawl', 'MaxTries')
    priority = 1

    for seed_host, url, second_chance_url in urls:
        ridealong = {'url': url, 'priority': priority, 'seed': True,
                     'retries_left': retries_left, 'seed_host': seed_host}
        if skip_crawled:
            ridealong['skip_crawled'] = skip_crawled
        if second_chance_url:
            ridealong['second_chance_url'] = second_chance_url
        if freeseedredirs:
            ridealong['freeredirs'] = freeseedredirs
        crawler.add_url(priority, ridealong)

    stats.stats_sum('seeds added', len(urls))
    return urls


def seed_from_redir(url):
    url_allowed.setup_seeds((url,))


def special_seed_handling(url):
    '''
    We don't expect seed-lists to be very clean: no scheme, etc.
    '''
    parts = urllib.parse.urlsplit(url)
    had_scheme = True
    if parts.scheme == '':
        had_scheme = False
        if url.startswith('//'):
            url = 'http:' + url
        else:
            url = 'http://' + url
    if parts.netloc.startswith('.'):
        # this looks ugly when it eventually causes dns to barf
        return None

    global POLICY
    if POLICY == 'www-then-non-www' and not had_scheme:
        # does hostname already have www? use URL() to find out
        temp = URL(url)
        if temp.hostname == temp.hostname_without_www:
            LOGGER.debug('adding a www to %s', url)
            if url.startswith('http://'):
                url = url.replace('http://', 'http://www.', 1)
            else:
                url = url.replace('https://', 'https://www.', 1)
    return url


def fail(ridealong, crawler, json_log):
    '''
    Called for all final failures
    '''
    if 'seed' not in ridealong:
        return

    url = ridealong['url']
    if 'second_chance_url' not in ridealong:
        LOGGER.info('Received a final failure for seed url %s', url.url)
        stats.stats_sum('seeds completely failed', 1)
        if json_log:
            json_log['seed_completely_failed'] = True
        return

    two = ridealong['second_chance_url']
    seed_host = ridealong.get('seed_host')
    if json_log:
        json_log['seed_second_chance_activated'] = True

    seed_some_urls(((seed_host, URL(two), None),), crawler, skip_crawled=True)
