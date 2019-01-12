from bs4 import BeautifulSoup
import cocrawler.parse as parse

html = '''
<html data-adblockkey="blah blah blah">
<head>
</head></head>
<body>
<a href="a-url">an-anchor</a>
</body>
</html>
'''

all_soup = BeautifulSoup(html, 'lxml')
print('all soup:', repr(all_soup))

# try to get the adblock key out
print('got data-adblockkey of', all_soup.get('data-adblockkey'))  # fails
html_soup = all_soup.find('html')
print('got data-adblockkey of', html_soup.get('data-adblockkey'))  # works

head, body = parse.split_head_body(html)
print('head', head)
head_soup = BeautifulSoup(head, 'lxml')
print('head soup:', repr(head_soup))

print()

print('body:', body)
body_soup = BeautifulSoup(body, 'lxml')
print('body soup:', repr(body_soup))

body = '''
<body>
<a href="a-url">an-anchor</a>
</body>
</html>
'''

print('body:', body)
body_soup = BeautifulSoup(body, 'lxml')
print('body soup:', repr(body_soup))

body = '''
</script>
<body>
<a href="a-url">an-anchor</a>
</body>
</html>
'''

print('body:', body)
body_soup = BeautifulSoup(body, 'lxml')
print('body soup:', repr(body_soup))

body = '''
</i>
<body>
<a href="a-url">an-anchor</a>
</body>
</html>
'''

print('body:', body)
body_soup = BeautifulSoup(body, 'lxml')
print('body soup:', repr(body_soup))

body = '''
<head>
</script>
</head>
<body>
<a href="a-url">an-anchor</a>
</body>
</html>
'''

print('body:', body)
body_soup = BeautifulSoup(body, 'lxml')
print('body soup:', repr(body_soup))
body = '''
<html>
</head>
<body>
<a href="a-url">an-anchor</a>
</body>
</html>
'''

print('body:', body)
body_soup = BeautifulSoup(body, 'lxml')
print('body soup:', repr(body_soup))
