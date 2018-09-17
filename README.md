# CoCrawler

[![Build Status](https://travis-ci.org/cocrawler/cocrawler.svg?branch=master)](https://travis-ci.org/cocrawler/cocrawler) [![Coverage Status](https://coveralls.io/repos/github/cocrawler/cocrawler/badge.svg?branch=master)](https://coveralls.io/github/cocrawler/cocrawler?branch=master) [![Apache License 2.0](https://img.shields.io/github/license/cocrawler/cocrawler.svg)](LICENSE)

CoCrawler is a versatile web crawler built using modern tools and
concurrency.

Crawling the web can be easy or hard, depending upon the details.
Mature crawlers like Nutch and Heritrix work great in many situations,
and fall short in others. Some of the most demanding crawl situations
include open-ended crawling of the whole web.

The object of this project is to create a modular crawler with
pluggable modules, capable of working well for a large variety of
crawl tasks. The core of the crawler is written in Python 3.5+ using
coroutines.

## Status

CoCrawler is pre-release, with major restructuring going on. It is
currently able to crawl at around 170 megabits / 170 pages/sec on a 4
core machine.

Screenshot: ![Screenshot](https://cloud.githubusercontent.com/assets/2142266/19621581/92e83044-9849-11e6-825d-66b674cc59f0.png "Screenshot")

## Installing

We recommend that you use pyenv, because (1) CoCrawler requires Python
3.5+, and (2) requirements.txt specifies exact module versions.

```
git clone https://github.com/cocrawler/cocrawler.git
cd cocrawler
make init  # will install requirements using pip
make pytest
make test_coverage
```

## Pluggable Modules

Pluggable modules make policy decisions, and use utility routines
to keep policy modules short and sweet.

An additional set of pluggable modules provide support for a variety
of databases. These databases are mostly used to orchestrate the
cooperation of multiple crawl processes, enabling the horizontal
scalability of the crawler over many cores and many nodes.

Crawled web assets are intended to be stored as WARC files, although
this interface should also pluggable.

## Ranking

Everyone knows that ranking is extremely important to search queries,
but it's also important to crawling. Crawling the most important stuff
is one of the best ways to avoid crawling too much webspam, soft 404s,
and crawler trap pages.

SEO is a multi-billion-dollar industry created to game search engine
ranking, and any crawl of a wide swath of the web is going to run into
poor-quality content attempting to appear to have high quality.
There's little chance that CoCrawler's algorithms will beat the most
sophisticated SEO techniques, but a little ranking goes a long way.

## Credits

CoCrawler draws on ideas from the Python 3.4 code in "500 Lines or
Less", which can be found at https://github.com/aosabook/500lines.
It is also heavily influenced by the experiences that Greg acquired
while working at blekko and the Internet Archive.

## License

Apache 2.0
