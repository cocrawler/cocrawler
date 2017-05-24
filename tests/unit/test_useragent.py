import copy
import pytest

import cocrawler.useragent as useragent
import cocrawler.config as config


def test_useragent():

    c = {'UserAgent': {'Style': 'crawler',
                       'MyPrefix': 'something',
                       'URL': 'http://example.com/cocrawler.html'}}
    config.set_config(c)
    version = '1.0'

    robotname, ua = useragent.useragent(version)

    assert version in ua
    assert 'http://example.com/cocrawler.html' in ua
    assert robotname == 'something-cocrawler'

    config.write('laptopplus', 'UserAgent', 'Style')
    robotname, ua = useragent.useragent(version)
    assert 'Mozilla/5.0' in ua

    config.write('tabletplus', 'UserAgent', 'Style')
    robotname, ua = useragent.useragent(version)
    assert 'Mozilla/5.0' in ua

    config.write('phoneplus', 'UserAgent', 'Style')
    robotname, ua = useragent.useragent(version)
    assert 'Mozilla/5.0' in ua

    config.set_config(c)
    config.write('error', 'UserAgent', 'Style')
    with pytest.raises(ValueError):
        robotname, ua = useragent.useragent(version)

    config.set_config(c)
    config.write('ha ha I left this off', 'UserAgent', 'URL')
    with pytest.raises(ValueError):
        robotname, ua = useragent.useragent(version)

    config.set_config(c)
    config.write('http://cocrawler.com/cocrawler.html', 'UserAgent', 'URL')
    with pytest.raises(ValueError):
        robotname, ua = useragent.useragent(version)

    config.set_config(c)
    config.write('test', 'UserAgent', 'MyPrefix')
    with pytest.raises(ValueError):
        robotname, ua = useragent.useragent(version)

    config.set_config(c)
    config.write('', 'UserAgent', 'MyPrefix')
    with pytest.raises(ValueError):
        robotname, ua = useragent.useragent(version)
