#uuid:...>
#WARC-DATE: ...

import sys

f1 = sys.argv[1]
f2 = sys.argv[2]

with open(f1, 'r') as fd1:
    contents1 = fd1.read()
with open(f1, 'r') as fd1:
    contents2 = fd1.read()


def munge(s):
    '''
    Remove things known to differ in WARC files:
    uuids
    WARC-Date: headers
    '''
    out = ''
    for line in s.split('\n'):
        if ':uuid:' in line:
            line, _, _ = line.partition(':uuid:')
        elif line.startswith('WARC-Date:'):
            line = 'WARC-Date:'
        out += line
    return out

if munge(contents1) == munge(contents2):
    sys.exit(0)
else:
    print('{} and {} differ'.format(f1, f2))
    sys.exit(1)
