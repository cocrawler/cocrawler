import sys
import difflib

f1 = sys.argv[1]
f2 = sys.argv[2]

with open(f1, 'r') as fd:
    contents1 = fd.read()
with open(f2, 'r') as fd:
    contents2 = fd.read()


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
        elif line.startswith('software:'):
            continue
        out += line + '\n'
    return out

m1 = munge(contents1)
m2 = munge(contents2)

if m1 == m2:
    sys.exit(0)

print('{} and {} differ'.format(f1, f2))

for line in difflib.unified_diff(m1.splitlines(), m2.splitlines(),
                                 fromfile=f1, tofile=f2):
    print(line)

sys.exit(1)
