import copy
import pytest

import cocrawler.useragent as useragent


def test_useragent():

    config = {'UserAgent': {'Style': 'crawler',
                            'MyPrefix': 'something',
                            'URL': 'http://example.com/cocrawler.html'}}

    version = '1.0'

    robotname, ua = useragent.useragent(config, version)

    assert version in ua
    assert 'http://example.com/cocrawler.html' in ua
    assert robotname == 'something-cocrawler'

    config['UserAgent']['Style'] = 'laptopplus'
    robotname, ua = useragent.useragent(config, version)
    assert 'Mozilla/5.0' in ua
    config['UserAgent']['Style'] = 'tabletplus'
    robotname, ua = useragent.useragent(config, version)
    assert 'Mozilla/5.0' in ua
    config['UserAgent']['Style'] = 'phoneplus'
    robotname, ua = useragent.useragent(config, version)
    assert 'Mozilla/5.0' in ua

    bad_config = copy.deepcopy(config)
    bad_config['UserAgent']['Style'] = 'error'
    with pytest.raises(ValueError):
        robotname, ua = useragent.useragent(bad_config, version)

    bad_config = copy.deepcopy(config)
    bad_config['UserAgent']['URL'] = 'ha ha I left this off'
    with pytest.raises(ValueError):
        robotname, ua = useragent.useragent(bad_config, version)

    bad_config = copy.deepcopy(config)
    bad_config['UserAgent']['URL'] = 'http://cocrawler.com/cocrawler.html'
    with pytest.raises(ValueError):
        robotname, ua = useragent.useragent(bad_config, version)

    bad_config = copy.deepcopy(config)
    bad_config['UserAgent']['MyPrefix'] = 'test'
    with pytest.raises(ValueError):
        robotname, ua = useragent.useragent(bad_config, version)

    bad_config = copy.deepcopy(config)
    bad_config['UserAgent']['MyPrefix'] = ''
    with pytest.raises(ValueError):
        robotname, ua = useragent.useragent(bad_config, version)
