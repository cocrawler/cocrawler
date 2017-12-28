'''
Code related to cookies. The standard aiohttp cookie jar is the least
scalable possible implementation, examining every cookie for every web
request.

We want either to not do cookies at all, or have a per-paydomain
cookie jar. Selection of which strategy to use is by config file,
or by hueristic. For example, if we see a url redirecting to itself
and setting cookies, we should try a cookie jar to see if that
works better.
'''


class DefectiveCookieJar:
    '''
    Defective cookie jar loses cookies.
    '''
    def __init__(self, unsafe=False):
        pass

    def __iter__(self):
        return iter(dict())

    def __contains__(self, item):
        return False

    def __hash__(self):
        return 0

    def __len__(self):
        return 0

    def update_cookies(self, cookies, response_url=None):
        pass

    def filter_cookies(self, request_url):
        return None
