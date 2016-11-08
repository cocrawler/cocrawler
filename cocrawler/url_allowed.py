'''
Generic implementation of url_allowed.
'''

import logging

LOGGER = logging.getLogger(__name__)

POLICY = None
SEEDS = set()

allowed_schemes = set(('http', 'https'))


def scheme_allowed(url):
    if url.urlparse.scheme not in allowed_schemes:
        return False
    return True

not_text_extension = set(('jpg', 'jpeg', 'png', 'gif',
                          'mp3', 'mid', 'midi',
                          'pdf', 'ps',
                          'gz', 'tar', 'tgz', 'zip',
                          'doc', 'docx', 'ppt', 'pptx'))


def extension_allowed(url):
    # part of a html-only policy XXX
    if url.urlparse.path:
        if url.urlparse.path.endswith('/'):
            return True
        _, last_part = url.urlparse.path.rsplit('/', maxsplit=1)
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
        return False  # cheating :-)
    elif POLICY == 'AllDomains':
        pass
    else:
        raise ValueError('unknown url_allowed policy of ' + str(POLICY))

    if not extension_allowed(url):
        return False

    return True

valid_policies = set(('SeedsDomain', 'SeedsHostname', 'OnlySeeds', 'AllDomains'))


def setup(seeds, config):
    global POLICY
    POLICY = config.get('Plugins', {})['url_allowed']

    if POLICY not in valid_policies:
        raise ValueError('unknown url_allowed policy of ' + str(POLICY))

    if POLICY == 'SeedsDomain':
        for s in seeds:
            SEEDS.add(s.registered_domain)
    elif POLICY == 'SeedsHostname':
        for s in seeds:
            SEEDS.add(s.hostname_without_www)

    LOGGER.debug('Seed list:')
    for s in SEEDS:
        LOGGER.debug('  Seed: %s', s)
