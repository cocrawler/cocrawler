import pytest

import stats

def test_max():
    stats.stats_max('foo', 3)
    stats.stats_max('bar', 2)
    stats.stats_max('foo', 5)
    assert stats.stat_value('foo') == 5
    assert stats.stat_value('bar') == 2

def test_sum():
    stats.stats_sum('foo2', 3)
    stats.stats_sum('bar2', 2)
    stats.stats_sum('foo2', 5)
    assert stats.stat_value('foo2') == 8
    assert stats.stat_value('bar2') == 2

