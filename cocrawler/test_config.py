import pytest

import config

def test_merge_dicts():
    a = {'a': {'a': 1}}
    b = {'b': {'b': 2}}

    c = config.merge_dicts(a, b)

    assert c == {'a': {'a': 1}, 'b': {'b': 2}}

    a = {'a': {'a': 1}, 'b': {'c': 3}}
    c = config.merge_dicts(a, b)

    assert c == {'a': {'a': 1}, 'b': {'b': 2, 'c': 3}}
