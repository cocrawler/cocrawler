#!/bin/sh

# make sure we exit immediately when there's an error, for CI:
set -e

# start a webserver
python ./mock-webserver.py > /dev/null 2>&1 &

# if COVERAGE is set, use it, else python
if [ -z "$COVERAGE" ]; then COVERAGE=python; fi

echo test-deep
echo
$COVERAGE ../cocrawler/crawl.py --configfile test-deep.yml
# tests against the logfiles
grep -q "/denied/" robotslog.jsonl || (echo "FAIL: nothing about /denied/ in robotslog"; exit 1)
(grep "/denied/" crawllog.jsonl | grep -q -v '"robots"' ) && (echo "FAIL: should not have seen /denied/ in crawllog.jsonl"; exit 1)

echo
echo test-wide
echo
$COVERAGE ../cocrawler/crawl.py --configfile test-wide.yml --config Testing.doesnotexist:1

# remove logfiles
#rm -f robotslog.jsonl crawllog.jsonl

# tear down the webserver. fails in travis, so ignore
kill %1 || true
