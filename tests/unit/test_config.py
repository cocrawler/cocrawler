import cocrawler.config as config


def test_merge_dicts():
    a = {'a': {'a': 1}}
    b = {'b': {'b': 2}}

    c = config.merge_dicts(a, b)

    assert c == {'a': {'a': 1}, 'b': {'b': 2}}

    a = {'a': {'a': 1}, 'b': {'c': 3}}
    c = config.merge_dicts(a, b)

    assert c == {'a': {'a': 1}, 'b': {'b': 2, 'c': 3}}


def test_type_fixup():
    tests = (('a', 'a'),
             ('a,b,c', 'a,b,c'),
             ('[a,b,c]', ['a', 'b', 'c']))

    for arg, result in tests:
        assert config.type_fixup(arg) == result
