.PHONY: init pytest test pylint clean_coverage test_coverage download-stuff run_parsers register distclean dist install mock-webserver

init:
	pip install -r requirements.txt
	#pip install git+https://github.com/yaml/pyyaml.git
	-pip install -r optional-requirements.txt

pur:
	pur
	pur -r optional-requirements.txt

pytest:
	tldextract -u -p  # update the database
	PYTHONPATH=. py.test

test: pytest
	(cd tests; PYTHONPATH=.. ./test.sh)
	(cd tests/warc; PYTHONPATH=../.. ./test.sh)

pylint:
	PYTHONPATH=. pylint *.py

clean_coverage:
	rm -f .coverage
	rm -f .coverage.*
	rm -f tests/.coverage
	rm -f tests/.coverage.*
	rm -f tests/warc/.coverage
	rm -f tests/warc/.coverage.*

test_coverage: clean_coverage
	tldextract -u -p  # update the database
	PYTHONPATH=. py.test --cov-report= --cov-append --cov cocrawler tests
	PYTHONPATH=. coverage run -a --source=cocrawler,scripts scripts/crawl.py --printdefault | wc -l | awk '{ if( $$1 > 10) {exit 0;} else {exit 1;} }'
	PYTHONPATH=. coverage run -a --source=cocrawler,scripts scripts/parse-html.py data/html-parsing-test.html > /dev/null
	(cd tests; PYTHONPATH=.. COVERAGE='coverage run -a --source=../cocrawler,../scripts' ./test.sh)
	(cd tests/warc; PYTHONPATH=../.. COVERAGE='coverage run -a --source=../../cocrawler,.' ./test.sh)
	coverage combine .coverage tests/.coverage tests/warc/.coverage
	coverage report

download-stuff:
	(cd data; wget -N https://geolite.maxmind.com/download/geoip/database/GeoLite2-City.tar.gz)
	(cd data; wget -N https://geolite.maxmind.com/download/geoip/database/GeoLite2-Country.tar.gz)
	(cd data; wget -N https://geolite.maxmind.com/download/geoip/database/GeoLite2-ASN.tar.gz)
	(cd data; wget -N https://ip-ranges.amazonaws.com/ip-ranges.json)

run_parsers:
	python ./scripts/run_parsers.py ~/public_html/

register:
	python setup.py register -r https://pypi.python.org/pypi

distclean:
	rm dist/*

dist: distclean
	python ./setup.py --long-description | rst2html --exit-status=2 2>&1 > /dev/null
	python ./setup.py bdist_wheel
	twine upload dist/* -r pypi

install:
	python ./setup.py install

mock-webserver:
	-pkill -U $$USER -f mock-webserver.py
	(cd tests; python -u ./mock-webserver.py 2>&1 | grep -v '" 200 ') &
