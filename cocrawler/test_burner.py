import functools
import burner

def trivial():
    return 42,

def test_stats_wrap():
    '''
    This code only runs in a separate thread, so pytest doesn't do coverage
    for it, even though my test suite does test it. Do a trivial test here.
    '''
    partial = functools.partial(trivial)

    burner.stats_wrap(partial, 'trivial')
