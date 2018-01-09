import cocrawler.seeds as seeds


def test_special_seed_handling():
    specialsh = seeds.special_seed_handling
    assert specialsh('foo').url == 'http://foo/'
    assert specialsh('//foo/').url == 'http://foo/'
    assert specialsh('https://foo').url == 'https://foo/'
    #assert specialsh('mailto:foo').url == 'mailto:foo'
