# CoCrawler

[![Build Status](https://github.com/cocrawler/cocrawler/actions/workflows/test-all.yml/badge.svg)](https://github.com/cocrawler/cocrawler/actions/workflows/test-all.yml) [![Coverage Status](https://coveralls.io/repos/github/cocrawler/cocrawler/badge.svg?branch=main)](https://coveralls.io/github/cocrawler/cocrawler?branch=main) [![Apache License 2.0](https://img.shields.io/github/license/cocrawler/cocrawler.svg)](LICENSE)

CoCrawler is a versatile web crawler built using modern tools and
concurrency.

Crawling the web can be easy or hard, depending upon the details.
Mature crawlers like Nutch and Heritrix work great in many situations,
and fall short in others. Some of the most demanding crawl situations
include open-ended crawling of the whole web.

The object of this project is to create a modular crawler with
pluggable modules, capable of working well for a large variety of
crawl tasks. The core of the crawler is written in Python 3.7+ using
coroutines.

## Status

CoCrawler is pre-release, with major restructuring going on. It is
currently able to crawl at around 170 megabits / 170 pages/sec on a 4
core machine.

Screenshot: ![Screenshot](https://cloud.githubusercontent.com/assets/2142266/19621581/92e83044-9849-11e6-825d-66b674cc59f0.png "Screenshot")

## Installing

We recommend that you use pyenv / virtualenv to separate the python
executables and packages used by cocrawler from everything else.

You can install cocrawler from pypi using "pip install cocrawler".

For a more fresh version, clone the repo and install like this:

```
git clone https://github.com/cocrawler/cocrawler.git
cd cocrawler
pip install . .[test]
make pytest
make test_coverage
```

The CI for this repo uses the latest versions of everything.  To see
exactly what worked last, click on the "Build Status" link above.
Alternately, you can look at `requirements.txt` for a test combination
that I probably ran before checking in.

## Credits

CoCrawler draws on ideas from the Python 3.4 code in "500 Lines or
Less", which can be found at https://github.com/aosabook/500lines.
It is also heavily influenced by the experiences that Greg acquired
while working at blekko and the Internet Archive.

## License

Apache 2.0
