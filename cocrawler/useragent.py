'''
Code that sets up a HTTP Client UserAgent string

There are 2 aspects of a user-agent:

1) The string that a crawler checks for in robots.txt
2) The User-Agent HTTP header that the crawler presents to websites

(2) should contain an URL, which should explain the purpose of the
crawler, and give instructions for how to block the crawler using
robots.txt and (1).

I don't want your crawling mistakes to cause my crawling to get
blocked, and vice versa. So I recommend that we all use different
strings for (1) and (2). The configuration for this is rougly:

UserAgent:
  Style: laptopplus
  MyPrefix: test
  URL: http://cocrawler.com/cocrawler.html

This produces a string of 'test-cocrawler' for (1), and
the user agent for (2) will contain test-cocrawler/VERSION
and the URL.

Choices for Style: laptopplus, tabletplus, phoneplus, crawler
'''

from . import config

laptop = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:59.0) Gecko/20100101 Firefox/59.0'
tablet = 'Mozilla/5.0 (iPad; CPU OS 11_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.0 Mobile/15E148 Safari/604.1'
phone = 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.0 Mobile/15E148 Safari/604.1'


def useragent(version):
    uac = config.read('UserAgent')

    basic = '{}-cocrawler/{} (+{})'.format(uac['MyPrefix'], version, uac['URL'])

    style = uac['Style']

    # check a few things I'd like to encourage / discourage
    if 'cocrawler.com' in basic:
        raise ValueError('Hey! Please point this thing at your own domain!')
    if '+http://' not in basic and '+https://' not in basic:
        raise ValueError('Please put a valid url into URL')
    if len(uac['MyPrefix']) == 0:
        raise ValueError('Please specify an actual prefix.')
    if uac['MyPrefix'] == 'test':
        raise ValueError('Please specify an actual prefix.')

    robotname = uac['MyPrefix'] + '-cocrawler'

    if style == 'crawler':
        return robotname, basic
    elif style == 'laptopplus':
        return robotname, laptop + ' ' + basic
    elif style == 'tabletplus':
        return robotname, tablet + ' ' + basic
    elif style == 'phoneplus':
        return robotname, phone + ' ' + basic
    else:
        raise ValueError('Unknown style of %s', style)
