import logging

from .urls import URL

LOGGER = logging.getLogger(__name__)


def expand_seeds(seeds):
    ret = []

    if seeds is None:
        return ret

    if seeds.get('Hosts', []):
        for h in seeds['Hosts']:
            ret.append(h)

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
                    ret.append(line.strip())

    # sitemaps are a little tedious, so I'll implement later.
    # needs to be fetched and then xml parsed and then <urlset ><url><loc></loc> elements extracted

    seeds = []
    for r in ret:
        seeds.append(URL(r, seed=True))

    return seeds
