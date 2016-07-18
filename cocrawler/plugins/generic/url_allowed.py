'''
Generic implementation of url_allowed.
'''

import urllib.parse
import tldextract
import unittest

POLICY=None
SEEDS=set()

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

def scheme_allowed(parts):
    if parts.scheme not in set(('http', 'https')):
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

def setup(parent, config):
    parent.register_plugin('url_allowed', url_allowed)
    seeds = parent.seeds
    global POLICY
    POLICY = config.get('Plugins', {})['url_allowed']

    for s in seeds:
        if POLICY == 'SeedsDomain':
            SEEDS.add(get_domain(s))
        elif POLICY == 'SeedsHostname':
            SEEDS.add(get_hostname(s))
        elif POLICY == 'OnlySeeds':
            pass
        elif POLICY == 'AllDomains':
            pass
        else:
            raise ValueError('unknown url_allowed policy of ' + str(POLICY))

class TestUrlAlowed(unittest.TestCase):
    def test_get_domain(self):
        self.assertEqual(get_domain('http://www.bbc.co.uk'), 'bbc.co.uk')
        self.assertEqual(get_domain('http://www.nhs.uk'), 'nhs.uk') # nhs.uk is a public suffix!
        self.assertEqual(get_domain('http://www.example.com'), 'example.com')
        self.assertEqual(get_domain('http://sub.example.com'), 'example.com')

    def test_gethostname(self):
        self.assertEqual(get_hostname('http://www.bbc.co.uk'), 'bbc.co.uk')
        self.assertEqual(get_hostname('http://www.example.com'), 'example.com')
        self.assertEqual(get_hostname('http://www.example.com:80'), 'example.com:80')
        self.assertEqual(get_hostname('http://bbc.co.uk'), 'bbc.co.uk')
        self.assertEqual(get_hostname('http://www.sub.example.com'), 'sub.example.com')
        self.assertEqual(get_hostname('http://sub.example.com'), 'sub.example.com')

    def test_url_allowed(self):
        self.assertFalse(url_allowed('ftp://example.com'))
        SEEDS.add('example.com')
        global POLICY
        POLICY = 'SeedsDomain'
        self.assertTrue(url_allowed('http://example.com'))
        self.assertTrue(url_allowed('http://sub.example.com'))
        POLICY = 'SeedsHostname'
        self.assertFalse(url_allowed('http://sub.example.com'))
        POLICY = 'OnlySeeds'
        self.assertFalse(url_allowed('http://example.com'))
        POLICY = 'AllDomains'
        self.assertTrue(url_allowed('http://example.com'))
        self.assertTrue(url_allowed('http://exa2mple.com'))
        self.assertTrue(url_allowed('http://exa3mple.com'))

    def test_scheme_allowed(self):
        self.assertTrue(scheme_allowed(urllib.parse.urlparse('http://example.com')))
        self.assertTrue(scheme_allowed(urllib.parse.urlparse('https://example.com')))
        self.assertFalse(scheme_allowed(urllib.parse.urlparse('ftp://example.com')))

    def test_extension_allowed(self):
        self.assertTrue(extension_allowed(urllib.parse.urlparse('https://example.com/')))
        self.assertTrue(extension_allowed(urllib.parse.urlparse('https://example.com/thing.with.dots/')))
        self.assertTrue(extension_allowed(urllib.parse.urlparse('https://example.com/thing.with.dots')))
        self.assertTrue(extension_allowed(urllib.parse.urlparse('https://example.com/index.html')))
        self.assertFalse(extension_allowed(urllib.parse.urlparse('https://example.com/foo.jpg')))
        self.assertFalse(extension_allowed(urllib.parse.urlparse('https://example.com/foo.tar.gz')))

if __name__ == '__main__':
    unittest.main()
