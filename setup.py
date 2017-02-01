#!/usr/bin/env python

import os
import re
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

requires = []
test_requirements = ['pytest>=3.0.0', 'coverage', 'pytest-cov']

scripts = ['scripts/bench_burner.py',
           'scripts/bench_dns.py',
           'scripts/crawl.py',
           'scripts/run_burner_bench.py',
           'scripts/run_burner.py',
           'scripts/run_parsers.py']

with open('cocrawler/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

if not version:
    raise RuntimeError('Cannot find version information')

with open('README.md', 'r') as f:
    readme = f.read()

# XXX need to add data_files for all the crap that's text

setup(
    name='cocrawler',
    version=version,
    description='A modern web crawler framework for Python',
    long_description=readme,
    author='Greg Lindahl and others',
    author_email='lindahl@pbm.com',
    url='https://github.com/cocrawler/cocrawler',
    packages=packages,
    install_requires=requires,
    scripts=scripts,
    license='Apache 2.0',
    zip_safe=True,
    classifiers=(
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
    ),
    cmdclass={'test': PyTest},
    tests_require=test_requirements,
)
