#!/usr/bin/env python

import sys

from setuptools import setup
from setuptools.command.test import test as TestCommand


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass into py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest

        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


packages = [
    'cocrawler',
]

requires = [
    'uvloop',
    'aiohttp',
    'aiodns',
    'pyyaml',
    'cchardet',
    'surt',
    'robotexclusionrulesparser',
    'cachetools',
    'filemagic',
    'tldextract',
    'sortedcontainers',
    'sortedcollections',
    'psutil',
    'hdrhistogram',
    'fakeredis',
    'beautifulsoup4',
    'lxml',
    'extensions',
    'warcio',
    'geoip2']

test_requirements = ['bottle', 'pytest>=3.0.0', 'coverage', 'pytest-cov']

scripts = ['scripts/aiohttp-fetch.py',
           'scripts/bench_burner.py',
           'scripts/bench_dns.py',
           'scripts/crawl.py',
           'scripts/dns-fetch.py',
           'scripts/parse-html.py',
           'scripts/run_burner_bench.py',
           'scripts/run_burner.py',
           'scripts/run_parsers.py',
           'scripts/cocrawler-savefile-dump.py']

try:
    import pypandoc
    description = pypandoc.convert_file('README.md', 'rst')
except (IOError, ImportError):
    description = open('README.md').read()

setup(
    name='cocrawler',
    use_scm_version=True,
    description='A modern web crawler framework for Python',
    long_description=description,
    author='Greg Lindahl and others',
    author_email='lindahl@pbm.com',
    url='https://github.com/cocrawler/cocrawler',
    packages=packages,
    setup_requires=['setuptools_scm'],
    install_requires=requires,
    scripts=scripts,
    license='Apache 2.0',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Console',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3 :: Only',
    ],
    cmdclass={'test': PyTest},
    tests_require=test_requirements,
)
