'''
Generic implementation of url_allowed.
'''

import logging
from collections import defaultdict

from . import config
from . import memory

LOGGER = logging.getLogger(__name__)

POLICY = None
SEEDS = None

allowed_schemes = set(('http', 'https'))


def scheme_allowed(url):
    if url.urlsplit.scheme not in allowed_schemes:
        return False
    return True


# not yet used
video_extension = set(('3gp', 'af', 'asf', 'avchd', 'avi', 'cam', 'dsh', 'flv', 'm1v', 'm2v',
                       'fla', 'flr', 'm4v', 'mkv', 'sol', 'wrap', 'mng', 'mov', 'mpg', 'mpeg',
                       'mp4', 'mpe', 'mxf', 'nsv', 'ogg', 'rm', 'svi', 'smi', 'wmv', 'webm'))

not_text_extension = set(('jpg', 'jpeg', 'png', 'gif', 'webp', 'svg',
                          'mp3', 'mid', 'midi',
                          'pdf', 'ps',
                          'gz', 'bz2', 'tar', 'tgz', 'zip', 'rar',
                          'doc', 'docx', 'ppt', 'pptx', 'xls', 'xlsx',
                          'odt', 'fodt', 'odp', 'fodp', 'ods', 'fods', 'odg', 'fodg', 'odf',
                          'swf'))

# not yet used
text_extension = set(('txt', 'html', 'php', 'htm', 'aspx', 'asp', 'shtml', 'jsp'))
text_embed_extension = set(('js', 'css'))


def extension_allowed(url):
    # part of a html-only policy XXX
    if url.urlsplit.path:
        if url.urlsplit.path.endswith('/'):
            return True
        _, last_part = url.urlsplit.path.rsplit('/', maxsplit=1)
        if last_part and '.' in last_part:
            _, extension = last_part.rsplit('.', maxsplit=1)
            # people use dots in random ways, so let's use a blocklist
            if extension.lower() in not_text_extension:
                return False
    return True


def host_prefix_match(url, SEEDS):
    hostprefixes = SEEDS[url.hostname_without_www]
    path = url.urlsplit.path
    for hp in hostprefixes:
        if path.startswith(hp):
            return True


def url_allowed(url):
    if not scheme_allowed(url):
        return False

    if POLICY == 'SeedsDomain':
        if url.registered_domain not in SEEDS:
            return False
    elif POLICY == 'SeedsHostname':
        if url.hostname_without_www not in SEEDS:
            return False
    elif POLICY == 'SeedsPrefix':
        if url.hostname_without_www not in SEEDS:
            return False
        if not host_prefix_match(url, SEEDS):
            return False
    elif POLICY == 'OnlySeeds':
        if url.url not in SEEDS:
            return False
    elif POLICY == 'AllDomains':
        pass
    else:
        raise ValueError('unknown url_allowed policy of ' + str(POLICY))

    if not extension_allowed(url):
        return False

    return url


valid_policies = {'SeedsDomain': set(), 'SeedsHostname': set(), 'SeedsPrefix': defaultdict(set),
                  'OnlySeeds': set(), 'AllDomains': None}


def setup(policy=None):
    global POLICY
    if policy:
        POLICY = policy
    else:
        POLICY = config.read('Plugins', 'url_allowed')

    if POLICY not in valid_policies:
        raise ValueError('unknown url_allowed policy of ' + str(POLICY))
    LOGGER.info('url_allowed policy: %s', POLICY)

    global SEEDS
    if valid_policies[POLICY] is not None:
        SEEDS = valid_policies[POLICY].copy()
    else:
        SEEDS = None

    memory.register_debug(mymemory)


def setup_seeds(seeds):
    if POLICY == 'SeedsDomain':
        for s in seeds:
            SEEDS.add(s.registered_domain)
    elif POLICY == 'SeedsHostname':
        for s in seeds:
            SEEDS.add(s.hostname_without_www)
    elif POLICY == 'SeedsPrefix':
        for s in seeds:
            SEEDS[s.hostname_without_www].add(s.urlsplit.path)
        # get rid of longer duplicates
        for h in SEEDS:
            print('trimming host', h)
            for s1 in list(SEEDS[h]):
                for s2 in list(SEEDS[h]):
                    print('checking', s1, s2)
                    if s1 != s2 and s1.startswith(s2):
                        print('dumping seed', s1)
                        SEEDS[h].discard(s1)
    elif POLICY == 'OnlySeeds':
        for s in seeds:
            SEEDS.add(s.url)

    if LOGGER.isEnabledFor(logging.DEBUG):
        LOGGER.debug('Seed list:')
        for s in seeds:  # only print the new ones
            LOGGER.debug('  Seed: %s', s)


def mymemory():
        '''
        Return a dict summarizing the our memory usage
        '''
        seeds = {}
        seeds['bytes'] = memory.total_size(SEEDS)
        seeds['len'] = len(SEEDS)
        return {'seeds': seeds}
