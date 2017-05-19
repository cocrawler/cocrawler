'''
A basic implementation of Sort-friendly URI Reordering Transforms

Since SURTs are only used internally, we don't have to use the same
algorithm as everyone else. However, the algorithm we pick governs
how much duplicate crawling we might do, vs webpages we might never
crawl because we thought we'd already seen them.

https://github.com/internetarchive/surt/

TODO: properly canonicalize urls before we get to SURT
       drop session ids
       make sure % escaping is minimized to only required %s
       punycode hostnames

TODO: extend to cover user, pass, port;
      downcase %encoded utf8;
      fixcase, not downcase (turkish);
      deal with encodings like latin-1, which the canonicalizer should leave as-is
'''

import urllib
import re
import unicodedata
import logging

import tldextract

LOGGER = logging.getLogger(__name__)


def parse_netloc(netloc):
    if '@' in netloc:
        userpassword, _, netloc = netloc.partition('@')
        if ':' in userpassword:
            user, password = userpassword.split(':', 1)
        else:
            user = userpassword
            password = ''
    else:
        user = ''
        password = ''
    if ':' in netloc:
        if (('[' in netloc and ']' not in netloc or
             ']' in netloc and '[' not in netloc)):
            # invalid ipv6 address. don't try to get a port
            hostname = netloc
            port = ''
        elif '[' in netloc:
            # valid ipv6 address
            if netloc.endswith(']'):
                hostname = netloc
                port = ''
            else:
                hostname, _, port = netloc.rpartition(':')
        else:
            hostname, _, port = netloc.rpartition(':')
    else:
        hostname = netloc
        port = ''
    return user, password, hostname, port


def discard_www_from_hostname(hostname):
    '''
    Useful for treating www.example.com the same as example.com

    ibm.com and hp.com are famous for using www3.ibm.com etc as equivalents to ibm.com,
    it was the lazy way to load-balance when the web was new. Now there are a bazillion links
    to these hosts out there in the wild, so we also remove www<small integer>.foo.com

    Traditionally, this transformation has been blindly applied and thus breaks hosts like www1.com.
    We use the public suffix list to avoid doing that.
    '''

    if not hostname.lower().startswith('www'):
        return hostname

    if tldextract.extract(hostname).registered_domain == hostname:
        return hostname

    return re.sub(r'^www\d{0,2}\.', '', hostname, count=1, flags=re.I)


def hostname_to_punycanon(hostname):
    '''
    Hostnames are complicated. They may be ascii, latin-1, or utf8. The
    incoming hostname might still have % escapes.
    '''

    hostname = hostname.rstrip('.')

    try:
        unquoted = urllib.parse.unquote(hostname, encoding='utf-8', errors='strict')
    except UnicodeDecodeError:
        try:
            unquoted = urllib.parse.unquote(hostname, encoding='iso-8859-1', errors='strict')
        except UnicodeDecodeError:
            # we are stuck. don't unquote.
            LOGGER.error('encoding of hostname {} confused me'.format(hostname))
            # return immediately without further processing (which would fail)
            return hostname

    # NFKC is recommended by Yahoo, didn't look farther on the Internets to see if this is obsolete advice
    unquoted = unicodedata.normalize('NFKC', unquoted)

    unquoted = unquoted.lower()  # XXX this will mangle a few Turkic letters, alas

    try:
        puny = unquoted.encode('ascii')
    except UnicodeEncodeError:
        try:
            puny = unquoted.encode('idna', errors='strict')
        except UnicodeError:
            LOGGER.error('failed trying to puny-code hostname {}'.format(unquoted))
            # return immediately because we can't punycode
            return unquoted

    # puny is now bytes, but they're ascii bytes.
    return puny.decode('ascii')


def reverse_hostname_parts(hostname):
    hostname = hostname.rstrip('.')

    # if the hostname is an [ipv6] or ipv4, do not reverse it.
    if hostname.startswith('[') and hostname.endswith(']'):  # ipv6 SHOULD have []
        return [hostname]
    if re.match(r'[1-9][0-9]{0,1}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$', hostname):
        return [hostname]

    parts = hostname.split('.')
    parts.reverse()

    return [p for p in parts]


standard_ports = {'http': '80', 'https': '443'}


def surt(url, parts=None):
    if parts is None:
        parts = urllib.parse.urlparse(url)

    # notes are how IA does it
    # scheme -- ignored, so https had better have the same content as http
    # netloc: user pass hostname port
    #  user:pass ignored
    #  hostname might be ip addr, leave that alone
    #   loses leading 'www.' or 'www\d+.' if present; lowercased; split and reversed
    #   punycode if necessary
    #  port is ignored
    # path and params are downcased
    # query is split on '&' and sorted
    # fragment is dropped

    (scheme, netloc, path, params, query, fragment) = parts

    scheme = scheme.lower()

    # urlparse lacks a parser to split 'user:password@host.name:port'
    (user, password, hostname, port) = parse_netloc(netloc)
    if standard_ports.get(scheme) == port:
        port = ''

    hostname = discard_www_from_hostname(hostname)
    hostname = hostname_to_punycanon(hostname)
    hostname = reverse_hostname_parts(hostname)

    if path == '/':
        path = ''
    path = path.lower()

    params = params.lower()

    # XXX query

    fragment = ''

    return urllib.parse.urlunparse((scheme, netloc, path, params, query, fragment))
