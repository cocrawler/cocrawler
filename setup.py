#!/usr/bin/env python

import sys
from os import path

from setuptools import setup
from setuptools.command.test import test as TestCommand


packages = [
    'cocrawler',
]

requires = [
    'uvloop',
    'aiohttp',
    'yarl',
    'aiodns',
    'pyyaml',
    'cchardet',
    'surt',
    'reppy',
    'cachetools',
    'tldextract>=3',
    'sortedcontainers',
    'sortedcollections',
    'psutil',
    'hdrhistogram',
    'beautifulsoup4',
    'lxml',
    'extensions',
    'warcio',
    'geoip2',
    'objgraph',
    'brotlipy',
    'setuptools_scm',
]

test_requirements = [
    'bottle',
    'pytest>=3.0.0',
    'pytest-cov',
    'pytest-asyncio',
]

extras_require = {
    'test': test_requirements,  # setup no longer tests, so make them an extra
}

scripts = [
    'scripts/aiohttp-fetch.py',
    'scripts/bench_burner.py',
    'scripts/bench_dns.py',
    'scripts/crawl.py',
    'scripts/parse-html.py',
    'scripts/run_burner_bench.py',
    'scripts/run_burner.py',
    'scripts/run_parsers.py',
    'scripts/cocrawler-savefile-dump.py',
]

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    description = f.read()

setup(
    name='cocrawler',
    use_scm_version=True,
    description='A modern web crawler framework for Python',
    long_description=description,
    long_description_content_type='text/markdown',
    author='Greg Lindahl and others',
    author_email='lindahl@pbm.com',
    url='https://github.com/cocrawler/cocrawler',
    packages=packages,
    python_requires=">=3.6.3",
    extras_require=extras_require,
    install_requires=requires,
    scripts=scripts,
    license='Apache 2.0',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Console',
        'Framework :: AsyncIO',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3 :: Only',
    ],
)
