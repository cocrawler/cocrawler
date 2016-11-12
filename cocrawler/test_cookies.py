import cookies

def test_defective_cookie_jar():
    jar = cookies.DefectiveCookieJar()

    for c in jar:
        pass
    if 'foo' in jar:
        pass
    d = {}
    d[jar] = 1
    assert len(jar) == 0
    jar.update_cookies({}, response_url=None)
    assert jar.filter_cookies('http://example.com/') == None
