import robots

def test_robots():
    '''
    There's already end-to-end testing for the normal functionality.
    Exercise only the weird stuff here.
    '''
    r = robots.Robots('foo', None, None, {'Robots':{'MaxTries': 4}, 'Logging': {}})

    robots_txt = b'<'
    assert not r.is_plausible_robots('example.com', robots_txt, 1.0)

    robots_txt = b'\xef\xbb\xbf' # BOM
    assert r.is_plausible_robots('example.com', robots_txt, 1.0)
    robots_txt = b'\xfe\xff'
    assert r.is_plausible_robots('example.com', robots_txt, 1.0)
    robots_txt = b'\xff\xfe'
    assert r.is_plausible_robots('example.com', robots_txt, 1.0)

    robots_txt = b'' # application/x-empty
    assert r.is_plausible_robots('example.com', robots_txt, 1.0)
    #robots_txt = b'%PDF-1.3\n'
    #assert not r.is_plausible_robots('example.com', robots_txt, 1.0)

    robots_txt = b'x'*1000001
    assert not r.is_plausible_robots('example.com', robots_txt, 1.0)

    robots_txt = b'foo'
    assert r.is_plausible_robots('example.com', robots_txt, 1.0)
