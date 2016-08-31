import robots

r = robots.Robots('foo', None, None, {'Robots':{'MaxTries': 4}, 'Logging': {}})

def test_robots():
    '''
    There's already end-to-end testing for the normal functionality.
    Exercise only the weird stuff here.
    '''

    robots = b'<'
    assert r.is_plausible_robots('example.com', robots, 1.0) == False

    robots = b'\xef\xbb\xbf' # BOM
    assert r.is_plausible_robots('example.com', robots, 1.0) == True
    robots = b'\xfe\xff'
    assert r.is_plausible_robots('example.com', robots, 1.0) == True
    robots = b'\xff\xfe'
    assert r.is_plausible_robots('example.com', robots, 1.0) == True

    robots = b'' # application/x-empty
    assert r.is_plausible_robots('example.com', robots, 1.0) == True
    robots = b'%PDF-1.3\n'
    assert r.is_plausible_robots('example.com', robots, 1.0) == False

    robots = b'x'*1000001
    assert r.is_plausible_robots('example.com', robots, 1.0) == False

    robots = b'foo'
    assert r.is_plausible_robots('example.com', robots, 1.0) == True
