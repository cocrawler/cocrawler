import ipaddress
import json
import logging
import os.path
from collections import defaultdict

import geoip2.database
import geoip2.errors

from . import config

LOGGER = logging.getLogger(__name__)

geoip_country = None
geoip_as = None
special_by_asn = None


def init():
    datadir = config.read('GeoIP', 'DataDir')
    datadir = os.path.expanduser(os.path.expandvars(datadir))

    if not os.path.exists(datadir):
        LOGGER.info('No GeoIP data found.')
        return

    try:
        global geoip_country
        geoip_country = geoip2.database.Reader(os.path.join(datadir, 'GeoLite2-Country.mmdb'))
        LOGGER.info('Loaded MaxMind GeoLite2-Country epoch '+str(geoip_country.metadata().build_epoch))
    except Exception as e:
        LOGGER.info('Something failed loading GeoLite2-Country.mmdb: '+e)
    try:
        global geoip_as
        geoip_as = geoip2.database.Reader(os.path.join(datadir, 'GeoLite2-ASN.mmdb'))
        LOGGER.info('Loaded MaxMind GeoLite2-ASN epoch '+str(geoip_as.metadata().build_epoch))
    except Exception as e:
        LOGGER.info('Something failed loading GeoLite2-ASN.mmdb: '+e)

    special_ip_file = os.path.join(datadir, 'special-ips.json')
    if os.path.exists(special_ip_file):
        with open(special_ip_file, 'r') as f:
            global special_by_asn
            special_by_asn = json.load(f)

        for asn in special_by_asn:
            new = []
            for name, network in special_by_asn[asn]:
                new.append((name, ipaddress.ip_network(network)))
            special_by_asn[asn] = new

        LOGGER.info('Loaded special-ip file')


def lookup(ip):
    ret = {}
    if geoip_country:
        try:
            country_info = geoip_country.country(ip)
        except geoip2.errors.AddressNotFoundError:
            LOGGER.debug('ip %s not found in MaxMind Country db', ip)
        else:
            country = country_info.country
            # country_name = country.name  # needs postprocessing
            # Islamic Republic of Foo; Foo, Republic Of, etc.
            ret['geoip-country'] = country.iso_code

    if geoip_as:
        try:
            asn_info = geoip_as.asn(ip)
        except geoip2.errors.AddressNotFoundError:
            LOGGER.debug('ip %s not found in MaxMind ASN db', ip)
        else:
            ret['ip-asn'] = str(asn_info.autonomous_system_number)
            ret['ip-asn-org'] = asn_info.autonomous_system_organization

    if special_by_asn:
        if 'ip-asn' in ret:
            asn = ret['ip-asn']
        else:
            asn = '0'  # a convention used when generating special_by_asn

        ipobj = ipaddress.ip_address(ip)
        if asn in special_by_asn:  # keys are str() thanks to json
            for name, network in special_by_asn[asn]:
                if ipobj in network:
                    ret['ip-special'] = name

    return ret


def lookup_all(addrs, host_geoip):
    for a in addrs:
        host = a['host']
        host_geoip[host] = lookup(host)


def add_facets(facets, host_geoip):
    lists = defaultdict(list)

    keys = sorted(host_geoip.keys())  # stable sort order for plurals
    for ip in keys:
        value = host_geoip[ip]
        facets.append(('ip', ip))
        lists['ip'].append(ip)
        for key in ('ip-asn', 'ip-asn-org', 'geoip-country', 'ip-special'):
            if key in value:
                facets.append((key, value[key]))
                lists[key].append(value[key])

    for key, value in lists.items():
        if key == 'geoip-country':
            key = 'geoip-countrie'
        facets.append((key+'s', ','.join(value)))
