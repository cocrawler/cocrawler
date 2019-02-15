#!/bin/sh

# make sure we exit immediately when there's an error, for CI:
set -e

# if COVERAGE is set, use it, else python
if [ -z "$COVERAGE" ]; then COVERAGE=python; fi

echo
echo test-warc
echo

# pre-cleanup
rm -f CC-TEST-01-FOO-00001-*.warc
rm -f CC-TEST-01-FOO-00002-*.warc
rm -f CC-TEST-01-00001-*.warc.gz

$COVERAGE ./test-warc.py

# there are 3 output files to diff... move them to canonical names
mv CC-TEST-01-FOO-00001-*.warc CC-TEST-01-FOO-00001-hostname.warc
mv CC-TEST-01-FOO-00002-*.warc CC-TEST-01-FOO-00002-hostname.warc
mv CC-TEST-01-00001-*.warc.gz CC-TEST-01-00001-hostname.warc.gz

# diffs

$COVERAGE ./warc-diff.py CC-TEST-01-FOO-00001-hostname.warc.in CC-TEST-01-FOO-00001-hostname.warc
$COVERAGE ./warc-diff.py CC-TEST-01-FOO-00002-hostname.warc.in CC-TEST-01-FOO-00002-hostname.warc

rm -f CC-TEST-01-00001-hostname.warc.in
gunzip -S .gz.in --to-stdout CC-TEST-01-00001-hostname.warc.gz.in > CC-TEST-01-00001-hostname.warc.in
rm -f CC-TEST-01-00001-hostname.warc
gunzip CC-TEST-01-00001-hostname.warc.gz
$COVERAGE ./warc-diff.py CC-TEST-01-00001-hostname.warc.in CC-TEST-01-00001-hostname.warc

# recompress to see if the syntax is happy
rm -f foo.warc.gz
warcio recompress CC-TEST-01-00001-hostname.warc foo.warc.gz
if [ `warcio index CC-TEST-01-00001-hostname.warc | wc -l` != `warcio index foo.warc.gz | wc -l` ]; then
    echo "warc index size changed on recompress"
    exit 1
fi
rm -f foo.warc.gz

# cleanup
rm CC-TEST-01-FOO-00001-hostname.warc
rm CC-TEST-01-FOO-00002-hostname.warc
rm CC-TEST-01-00001-hostname.warc
rm CC-TEST-01-00001-hostname.warc.in

echo
echo reached warc/test.sh exit
echo

