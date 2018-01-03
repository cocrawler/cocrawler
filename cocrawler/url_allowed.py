'''
Generic implementation of url_allowed.
'''

import logging

from . import config

LOGGER = logging.getLogger(__name__)

POLICY = None
SEEDS = set()

allowed_schemes = set(('http', 'https'))


def scheme_allowed(url):
    if url.urlsplit.scheme not in allowed_schemes:
        return False
    return True

# not yet used
video_extension = set(('3gp', 'af', 'asf', 'avchd', 'avi', 'cam', 'dsh', 'flv', 'm1v', 'm2v',
                       'fla', 'flr', 'm4v', 'mkv', 'sol', 'wrap', 'mng', 'mov', 'mpg', 'mpeg',
                       'mp4', 'mpe', 'mxf', 'nsv', 'ogg', 'rm', 'svi', 'smi', 'wmv', 'webm'))

not_text_extension = set(('jpg', 'jpeg', 'png', 'gif',
                          'mp3', 'mid', 'midi',
                          'pdf', 'ps',
                          'gz', 'bz2', 'tar', 'tgz', 'zip', 'rar',
                          'doc', 'docx', 'ppt', 'pptx', 'xls', 'xlsx',
                          'swf'))

# not yet used
text_extension = set(('txt', 'html', 'php', 'htm', 'aspx', 'asp', 'shtml', 'jsp'))


def extension_allowed(url):
    # part of a html-only policy XXX
    if url.urlsplit.path:
        if url.urlsplit.path.endswith('/'):
            return True
        _, last_part = url.urlsplit.path.rsplit('/', maxsplit=1)
        if last_part and '.' in last_part:
            _, extension = last_part.rsplit('.', maxsplit=1)
            # people use dots in random ways, so let's use a blacklist
            if extension.lower() in not_text_extension:
                return False
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
    elif POLICY == 'OnlySeeds':
        if url.url not in SEEDS:
            return False
    elif POLICY == 'AllDomains':
        pass
    else:
        raise ValueError('unknown url_allowed policy of ' + str(POLICY))

    if not extension_allowed(url):
        return False

    return True


valid_policies = set(('SeedsDomain', 'SeedsHostname', 'OnlySeeds', 'AllDomains'))


def setup():
    global POLICY
    POLICY = config.read('Plugins', 'url_allowed')

    if POLICY not in valid_policies:
        raise ValueError('unknown url_allowed policy of ' + str(POLICY))
    LOGGER.info('url_allowed policy: %s', POLICY)


def setup_seeds(seeds):
    if POLICY == 'SeedsDomain':
        for s in seeds:
            SEEDS.add(s.registered_domain)
    elif POLICY == 'SeedsHostname':
        for s in seeds:
            SEEDS.add(s.hostname_without_www)
    elif POLICY == 'OnlySeeds':
        for s in seeds:
            SEEDS.add(s.url)

    if LOGGER.isEnabledFor(logging.DEBUG):
        LOGGER.debug('Seed list:')
        for s in seeds:  # only print the new ones
            LOGGER.debug('  Seed: %s', s)
