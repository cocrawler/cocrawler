import cocrawler.topk as topk


def test_topK_max():
    t = topk.topK_max(3)
    t.add('foo', 1, None)
    t.add('bar', 1, None)
    assert t.readout() == [('foo', [1, None]), ('bar', [1, None])]
    t.add('baz', 3, 'ridealong')
    assert t.readout() == [('baz', [3, 'ridealong']), ('foo', [1, None]), ('bar', [1, None])]
    t.add('bar', 2, 'bara')  # update value and ridealong
    assert t.readout() == [('baz', [3, 'ridealong']), ('bar', [2, 'bara']), ('foo', [1, None])]
    t.add('barf', 1, None)  # equal to smallest, nothing happens
    assert t.readout() == [('baz', [3, 'ridealong']), ('bar', [2, 'bara']), ('foo', [1, None])]
    t.add('barf', 2, None)  # evict foo
    assert t.readout() == [('baz', [3, 'ridealong']), ('bar', [2, 'bara']), ('barf', [2, None])]


def test_topK_sum():
    t = topk.topK_sum(3)
    t.add('foo', 1, None)
    t.add('bar', 1, None)
    assert t.readout() == [('foo', [1, None]), ('bar', [1, None])]
    t.add('baz', 3, 'ridealong')
    assert t.readout() == [('baz', [3, 'ridealong']), ('foo', [1, None]), ('bar', [1, None])]
    t.add('bar', 1, 'bara')  # update value and ridealong
    assert t.readout() == [('baz', [3, 'ridealong']), ('bar', [2, 'bara']), ('foo', [1, None])]
    t.add('barf', 1, None)  # equal to smallest, replacement happens
    assert t.readout() == [
        ('baz', [3, 'ridealong']), ('bar', [2, 'bara']), ('barf', [1, None])  # barf error is 0
        ]
    t.add('barf', 2, None)  # update
    assert t.readout() == [('baz', [3, 'ridealong']), ('barf', [3, None]), ('bar', [2, 'bara'])]
    t.add('blech', 1, None)  # evict bar
    assert t.readout() == [('baz', [3, 'ridealong']), ('barf', [3, None])]  # blech error is 2, invisible
