#!/bin/sh

# make sure we exit immediately when there's an error, for CI:
set -e

# start a webserver
(python -u ./mock-webserver.py 2>&1 | grep -v '" 200 ') &
# give it a chance to bind
sleep 1

# if COVERAGE is set, use it, else python
if [ -z "$COVERAGE" ]; then COVERAGE=python; fi

NOCH=--no-confighome

echo
echo test-deep
echo
$COVERAGE ../scripts/crawl.py --configfile test-deep.yml $NOCH
# tests against the logfiles
grep -q "/denied/" robotslog.jsonl || (echo "FAIL: nothing about /denied/ in robotslog"; exit 1)
(grep "/denied/" crawllog.jsonl | grep -q -v '"robots"' ) && (echo "FAIL: should not have seen /denied/ in crawllog.jsonl"; exit 1)
rm -f robotslog.jsonl crawllog.jsonl

echo
echo test-wide
echo
$COVERAGE ../scripts/crawl.py --configfile test-wide.yml --config Testing.doesnotexist:1 $NOCH
rm -f robotslog.jsonl crawllog.jsonl

echo
echo test-wide with save and load first half
echo
rm -f test-wide-save
cat test-wide.yml test-wide-save.yml > test-wide-tmp.yml
$COVERAGE ../scripts/crawl.py --configfile test-wide-tmp.yml --no-test --config Crawl.MaxCrawledUrls:5 --config Crawl.MaxWorkers:3 $NOCH
rm -f test-wide-tmp.yml

ls -l test-wide-save

echo
echo test wide save and load second half: load
echo
$COVERAGE ../scripts/crawl.py --configfile test-wide.yml --load test-wide-save $NOCH
rm -f test-wide-save
rm -f robotslog.jsonl crawllog.jsonl

echo
echo test-failures
echo
$COVERAGE ../scripts/crawl.py --configfile test-failures.yml --config error --config error:1 --config error.error:1 $NOCH
rm -f robotslog.jsonl crawllog.jsonl

# tear down the mock webserver a couple of ways
kill %1 || true
pkill -e -f mock-webserver.py || true

echo
echo run_burner
echo

$COVERAGE ../scripts/run_burner.py ./test_burner.html

echo
echo bench_burner
echo

$COVERAGE ../scripts/bench_burner.py --count 100
