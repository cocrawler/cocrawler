import cocrawler.seeds as seeds


def test_special_seed_handling():
    specialsh = seeds.special_seed_handling
    assert specialsh('foo') == 'http://foo'
    assert specialsh('//foo/') == 'http://foo/'
    assert specialsh('https://foo') == 'https://foo'
    #assert specialsh('mailto:foo') == 'mailto:foo'
