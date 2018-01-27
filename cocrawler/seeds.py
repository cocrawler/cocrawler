import urllib
import logging

from . import config
from . import stats
from .urls import URL
from . import url_allowed

LOGGER = logging.getLogger(__name__)

POLICY = None
valid_policies = set(('None', 'www-then-non-www'))


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
            urls.append(u)

    seed_files = seeds.get('Files', [])
    if seed_files:
        if not isinstance(seed_files, list):
            seed_files = [seed_files]
        for name in seed_files:
            name = str(name)  # yaml leaves filenames like '1000' as ints
            LOGGER.info('Loading seeds from file %s', name)
            with open(name, 'r') as f:
                for line in f:
                    if '#' in line:
                        line, _ = line.split('#', 1)
                    line = line.strip()
                    if line == '':
                        continue
                    u = special_seed_handling(line)
                    urls.append(u)

    # sitemaps are a little tedious, so I'll implement later.
    # needs to be fetched and then xml parsed and then <urlset ><url><loc></loc> elements extracted

    return seed_some_urls(urls, crawler)


def seed_some_urls(urls, crawler, second_chance=True):
    freeseedredirs = config.read('Seeds', 'FreeSeedRedirs')
    retries_left = config.read('Seeds', 'SeedRetries') or config.read('Crawl', 'MaxTries')
    priority = 1

    # url_allowed.setup_seeds(urls)  # add_url now does this

    for u in urls:
        ridealong = {'url': u, 'priority': priority, 'seed': True,
                     'skip_seen_url': True, 'retries_left': retries_left,
                     'original_url': u.url, 'second_chance': second_chance}
        if freeseedredirs:
            ridealong['free_redirs'] = freeseedredirs
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

    url = URL(url)
    return url


def fail(ridealong, crawler):
    '''
    Called for all final failures
    '''
    if 'seed' not in ridealong:
        return

    url = ridealong['url']

    if not ridealong.get('second_chance', False):
        return
    del ridealong['second_chance']

    LOGGER.info('Received a final failure for seed url %s', url.url)
    stats.stats_sum('seeds failed', 1)

    if POLICY == 'www-then-non-www':
        # url could have changed because of a redirect, that's why we saved it
        if 'original_url' not in ridealong:
            LOGGER.info('should have seen original_url in this seed, but did not')
            return

        original_url = ridealong['original_url']
        if 'www.' not in original_url:
            LOGGER.info('original url did not contain www, adding it')
            if original_url.startswith('https://'):
                url = URL(original_url.replace('https://', 'https://www.', 1))
            if original_url.startswith('http://'):
                url = URL(original_url.replace('http://', 'http://www.', 1))
        else:
            url = URL(original_url.replace('www.', '', 1))

        LOGGER.info('seed url second chance: %s to %s', original_url, url.url)
        stats.stats_sum('seeds second chances', 1)
        seed_some_urls((url,), crawler, second_chance=False)
