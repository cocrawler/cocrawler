'''
Generic implementation of url_allowed.
'''

import logging
import urllib.parse
import tldextract

LOGGER = logging.getLogger(__name__)

POLICY=None
SEEDS=set()

# XXX make a function to canonicalize urls and hostnames for comparison purposes (a la surt)

def get_domain(hostname):
    # XXX config option to set include_psl_private_domains=True ?
    #  sometimes we do want *.blogspot.com to all be different tlds
    tlde = tldextract.extract(hostname)
    mylist = list(tlde) # make it easy to change
    if mylist[1] == 'www':
        mylist[1] = ''
    if mylist[2] == 'www':
        mylist[2] = ''
    return '.'.join(part for part in mylist[1:3] if part)

def get_hostname(hostname, parts=None):
    if not parts:
        parts = urllib.parse.urlparse(hostname)
    hostname = parts.netloc
    if hostname.startswith('www.'):
        hostname = hostname[4:]
    return hostname

allowed_schemes = set(('http', 'https'))
def scheme_allowed(parts):
    if parts.scheme not in allowed_schemes:
        return False
    return True

not_text_extension = set(('jpg', 'jpeg', 'png', 'gif',
'mp3', 'mid', 'midi',
'pdf', 'ps',
'gz', 'tar', 'tgz', 'zip',
'doc', 'docx', 'ppt', 'pptx'))

def extension_allowed(parts):
    # part of a html-only policy XXX
    if parts.path:
        if parts.path.endswith('/'):
            return True
        parts, last_part = parts.path.rsplit('/', maxsplit=1)
        if last_part and '.' in last_part:
            name, extension = last_part.rsplit('.', maxsplit=1)
            # people use dots in random ways, so let's use a blacklist
            if extension.lower() in not_text_extension:
                return False
    return True

def url_allowed(url):
    parts = urllib.parse.urlparse(url)
    if not scheme_allowed(parts):
        return False
    if not extension_allowed(parts):
        return False

    if POLICY == 'SeedsDomain':
        return get_domain(url) in SEEDS
    elif POLICY == 'SeedsHostname':
        return get_hostname(url, parts=parts) in SEEDS
    elif POLICY == 'OnlySeeds':
        return False # cheating :-)
    elif POLICY == 'AllDomains':
        return True
    else:
        raise ValueError('unknown url_allowed policy of ' + str(POLICY))
    return False

valid_policies = set(('SeedsDomain', 'SeedsHostname', 'OnlySeeds', 'AllDomains'))

def setup(parent, config):
    parent.register_plugin('url_allowed', url_allowed)
    global POLICY
    POLICY = config.get('Plugins', {})['url_allowed']

    if POLICY not in valid_policies:
        raise ValueError('unknown url_allowed policy of ' + str(POLICY))

    if POLICY == 'SeedsDomain':
        seeds = parent._seeds
        for s in seeds:
            SEEDS.add(get_domain(s))
    elif POLICY == 'SeedsHostname':
        seeds = parent._seeds
        for s in seeds:
            SEEDS.add(get_hostname(s))

    LOGGER.debug('Seed list:')
    for s in SEEDS:
        LOGGER.debug('  Seed: %s', s)

