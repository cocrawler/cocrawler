import urllib
import logging

from . import stats
from .urls import URL
from . import url_allowed

LOGGER = logging.getLogger(__name__)


def expand_seeds_config(config, crawler):
    urls = []
    seeds = config.read('Seeds')

    if seeds is None:
        return

    if seeds.get('Hosts', []):
        for h in seeds['Hosts']:
            u = special_seed_handling(h)
            urls.append(URL(u))

    seed_files = seeds.get('Files', [])
    if seed_files:
        if not isinstance(seed_files, list):
            seed_files = [seed_files]
        for name in seed_files:
            LOGGER.info('Loading seeds from file %s', name)
            with open(name, 'r') as f:
                for line in f:
                    if '#' in line:
                        line, _ = line.split('#', 1)
                    if line.strip() == '':
                        continue
                    u = special_seed_handling(line.strip())
                    urls.append(URL(u))

    # sitemaps are a little tedious, so I'll implement later.
    # needs to be fetched and then xml parsed and then <urlset ><url><loc></loc> elements extracted

    return seed_some_urls(urls, config, crawler)


def seed_some_urls(urls, config, crawler):
    freeseedredirs = config.read('Seeds', 'FreeSeedRedirs')
    retries_left = config.read('Seeds', 'SeedRetries') or config.read('Crawl', 'MaxTries')
    priority = 1

    url_allowed.setup_seeds(urls)

    for u in urls:
        ridealong = {'url': u, 'priority': priority, 'seed': True,
                'skip_seen_url': True, 'retries_left': retries_left}
        if freeseedredirs:
            ridealong['free_redirs'] = freeseedredirs
        crawler.add_url(priority, ridealong)

    stats.stats_sum('added seeds', len(urls))
    return urls


def special_seed_handling(url):
    '''
    We don't expect seed-lists to be very clean: no scheme, etc.
    '''
    # use urlsplit to accurately test if a scheme is present
    parts = urllib.parse.urlsplit(url)
    if parts.scheme == '':
        if url.startswith('//'):
            url = 'http:' + url
        else:
            url = 'http://' + url
    return url
