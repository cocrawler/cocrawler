.PHONY: dist distclean

init:
	pip install -r requirements.txt

pytest:
	tldextract -u -p  # update the database
	PYTHONPATH=. py.test

test: pytest
	(cd tests; PYTHONPATH=.. ./test.sh)

pylint:
	PYTHONPATH=. pylint *.py

clean_coverage:
	rm -f .coverage
	rm -f .coverage.*
	rm -f tests/.coverage
	rm -f tests/.coverage.*

test_coverage: clean_coverage
	tldextract -u -p  # update the database
	PYTHONPATH=. py.test --cov-report= --cov-append --cov cocrawler tests
	PYTHONPATH=. coverage run -a --source=cocrawler,scripts scripts/crawl.py --printdefault | wc -l | awk '{ if( $$1 > 10) {exit 0;} else {exit 1;} }'
	(cd tests; PYTHONPATH=.. COVERAGE='coverage run -a --source=../cocrawler,../scripts' ./test.sh)
	coverage combine tests/.coverage .coverage
	coverage report

run_parsers:
	python ./run_parsers.py ~/public_html/

register:
	python setup.py register -r https://testpypi.python.org/pypi

distclean:
	rm dist/*

dist: distclean
	python ./setup.py bdist_wheel
	twine upload dist/* -r testpypi
