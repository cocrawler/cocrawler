#!/bin/sh

# make sure we exit immediately when there's an error, for CI:
set -e

# if COVERAGE is set, use it, else python
if [ -z "$COVERAGE" ]; then COVERAGE=python; fi

echo
echo test-warc
echo

$COVERAGE ./test-warc.py

# there are 3 output files to diff... move them to canonical names
mv -u CC-TEST-01-FOO-00001-*.warc CC-TEST-01-FOO-00001-rd.bx9.net.warc
mv -u CC-TEST-01-FOO-00002-*.warc CC-TEST-01-FOO-00002-rd.bx9.net.warc
mv -u CC-TEST-01-00001-*.warc.gz CC-TEST-01-00001-rd.bx9.net.warc.gz

$COVERAGE ./warc-diff.py CC-TEST-01-FOO-00001-rd.bx9.net.warc.in CC-TEST-01-FOO-00001-rd.bx9.net.warc
$COVERAGE ./warc-diff.py CC-TEST-01-FOO-00002-rd.bx9.net.warc.in CC-TEST-01-FOO-00002-rd.bx9.net.warc

rm -f CC-TEST-01-00001-rd.bx9.net.warc.in
gunzip -S .gz.in --to-stdout CC-TEST-01-00001-rd.bx9.net.warc.gz.in > CC-TEST-01-00001-rd.bx9.net.warc.in
rm -f CC-TEST-01-00001-rd.bx9.net.warc
gunzip CC-TEST-01-00001-rd.bx9.net.warc.gz
$COVERAGE ./warc-diff.py CC-TEST-01-00001-rd.bx9.net.warc.in CC-TEST-01-00001-rd.bx9.net.warc

# cleanup
rm CC-TEST-01-FOO-00001-rd.bx9.net.warc
rm CC-TEST-01-FOO-00002-rd.bx9.net.warc
rm CC-TEST-01-00001-rd.bx9.net.warc
rm CC-TEST-01-00001-rd.bx9.net.warc.in

echo
echo reached warc/test.sh exit
echo

