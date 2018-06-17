#!/bin/sh

# Clue: COCRAWLER_LOGLEVEL=3 environment variable

# make sure we exit immediately when there's an error, for CI:
set -e

# if there's a stray webserver, kill it
# would like to use pkill -e but this option is not in ubuntu 12.04
pkill -U $USER -f mock-webserver.py || true
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
rm -f robotslog.jsonl crawllog.jsonl Testing-000000-*.warc.gz frontierlog
$COVERAGE ../scripts/crawl.py --configfile test-deep.yml $NOCH --config WARC.WARCAll:True

# tests against the logfiles
grep -q "/denied/" robotslog.jsonl || (echo "FAIL: nothing about /denied/ in robotslog"; exit 1)
(grep "/denied/" crawllog.jsonl | grep -q -v '"robots"' ) && (echo "FAIL: should not have seen /denied/ in crawllog.jsonl"; exit 1)

echo
echo test-deep-warc
echo

# and the WARC
COUNT=`warcio index Testing-000000-*.warc.gz | wc -l`
if [ "$COUNT" != "1999" ]; then
   echo "warc index is the wrong size: saw $COUNT"
   exit 1
fi
rm -f testing.warc.gz
warcio recompress Testing-000000-*.warc.gz testing.warc.gz
if [ `warcio index Testing-000000-*.warc.gz | wc -l` != `warcio index testing.warc.gz | wc -l` ]; then
    echo "warc index size changed on recompress"
    exit 1
fi
echo OK
rm -f robotslog.jsonl crawllog.jsonl Testing-000000-*.warc.gz testing.warc.gz

echo
echo test-scheduler
echo
$COVERAGE ../scripts/crawl.py --configfile test-scheduler.yml $NOCH
rm -f robotslog.jsonl crawllog.jsonl

echo
echo test-wide
echo
$COVERAGE ../scripts/crawl.py --configfile test-wide.yml --config Testing.doesnotexist:1 $NOCH
rm -f robotslog.jsonl crawllog.jsonl facetlog.jsonl rejectedaddurl.log

echo
echo test-wide with save and load first half
echo skipped because Greg removed MaxCrawledUrls
echo
#rm -f test-wide-save
#cat test-wide.yml test-wide-save.yml > test-wide-tmp.yml
#$COVERAGE ../scripts/crawl.py --configfile test-wide-tmp.yml --no-test --config Crawl.GlobalBudget:5 --config Crawl.MaxWorkers:3 $NOCH
#rm -f test-wide-tmp.yml
## save these in case debugging is needed
#mv robotslog.jsonl robotslog.jsonl.save
#mv crawllog.jsonl crawllog.jsonl.save
#mv rejectedaddurl.log rejectedaddurl.log.save

#ls -l test-wide-save

#echo
#echo test wide save and load second half: load
#echo
#$COVERAGE ../scripts/crawl.py --configfile test-wide.yml --load test-wide-save $NOCH
#rm -f test-wide-save
#rm -f robotslog.jsonl.save crawllog.jsonl.save rejectedaddurl.log.save
#rm -f robotslog.jsonl crawllog.jsonl facetlog.jsonl rejectedaddurl.log

echo
echo test-failures
echo
$COVERAGE ../scripts/crawl.py --configfile test-failures.yml --config error --config error:1 --config error.error:1 $NOCH
rm -f robotslog.jsonl crawllog.jsonl

echo
echo aiohttp-fetch
echo
$COVERAGE ../scripts/aiohttp-fetch.py http://127.0.0.1:8080/hello > /dev/null
echo
echo aiohttp-fetch -- expect dns fail
echo
$COVERAGE ../scripts/aiohttp-fetch.py http://this-dns-lookup-will-fail-and-raise.com:8080/hello

echo
echo tearing down mock webserver
echo

# tear down the mock webserver a couple of ways
kill %1 || true
pkill -U $USER -f mock-webserver.py || true

echo
echo run_burner
echo

$COVERAGE ../scripts/run_burner.py ./test_burner.html

echo
echo bench_burner
echo

$COVERAGE ../scripts/bench_burner.py --count 100

echo
echo bench_dns check with bad nameserver, expected to say \'not suitable for crawling\'
echo

$COVERAGE ../scripts/bench_dns.py --count=3 --config Fetcher.Nameservers:4.2.2.1 --expect-not-suitable

echo
echo dns-fetch
echo

$COVERAGE ../scripts/dns-fetch.py example.com --config Fetcher.Nameservers:8.8.8.8

echo
echo reached test.sh exit
echo
