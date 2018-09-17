import time

import cocrawler.stats as stats


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


def test_set():
    stats.stats_set('foo3', 5)
    stats.stats_set('bar3', 2)
    stats.stats_set('foo3', 3)
    assert stats.stat_value('foo3') == 3
    assert stats.stat_value('bar2') == 2


def test_burn():
    with stats.record_burn('foo', url='http://example.com/'):
        t0 = time.process_time()
        while time.process_time() < t0 + 0.001:
            pass

    assert stats.burners['foo']['count'] == 1
    assert stats.burners['foo']['time'] > 0 and stats.burners['foo']['time'] < 0.3
    assert 'list' not in stats.burners['foo']  # first burn never goes on the list

    with stats.record_burn('foo', url='http://example.com/'):
        t0 = time.process_time()
        while time.process_time() < t0 + 0.2:
            pass

    assert stats.burners['foo']['count'] == 2
    assert stats.burners['foo']['time'] > 0 and stats.burners['foo']['time'] < 0.3
    assert len(stats.burners['foo']['list']) == 1

    stats.update_cpu_burn('foo', 3, 3.0, set())
    assert stats.burners['foo']['count'] == 5
    assert stats.burners['foo']['time'] > 3.0 and stats.burners['foo']['time'] < 3.3
    assert len(stats.burners['foo']['list']) == 1

    stats.report()


def test_latency():
    with stats.record_latency('foo', url='http://example.com/'):
        t0 = time.time()
        while time.time() < t0 + 0.001:
            pass

    assert stats.latencies['foo']['count'] == 1
    assert stats.latencies['foo']['time'] > 0 and stats.latencies['foo']['time'] < 0.3
    assert 'list' not in stats.latencies['foo']  # first latency never goes on the list
    assert 'hist' in stats.latencies['foo']

    with stats.record_latency('foo', url='http://example.com/', elapsedmin=0.1):
        time.sleep(0.3)

    assert stats.latencies['foo']['count'] == 2
    assert stats.latencies['foo']['time'] > 0 and stats.latencies['foo']['time'] < 20.0
    assert 'list' in stats.latencies['foo']

    stats.report()


def test_update():
    # I suppose the contents of stats.* depends on what order the tests are run in.
    # Looks like tests outside this file add data to stats.*, too
    l = stats.raw()
    m, s, b = l
    m = m.copy()
    s = s.copy()
    b = b.copy()
    stats.update(l)

    # hard to avoid being overly-intimate with the data structure to test it
    for k in m:
        assert m[k] == stats.stat_value(k)
        break
    for k in s:
        assert s[k]*2 == stats.stat_value(k)
        break
    # no test for b, yet
