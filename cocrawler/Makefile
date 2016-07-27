pytest:
	py.test

test: pytest
	(cd ../tests; ./test.sh)

pylint:
	PYTHONPATH=. pylint *.py

clean_coverage:
	rm -f .coverage
	rm -f .coverage.*
	rm -f ../tests/.coverage
	rm -f ../tests/.coverage.*

test_coverage: clean_coverage
	py.test --cov-report= --cov-append --cov stats --cov cocrawler --cov crawl --cov config --cov datalayer --cov fetcher --cov parse --cov robots --cov seeds --cov stats --cov url_allowed *.py plugins/generic/*.py -v
	coverage run -a --source=. crawl.py --printdefault | wc -l | awk '{ if( $$1 > 10) {exit 0;} else {exit 1;} }'
	(cd ../tests; COVERAGE='coverage run -a --source=../cocrawler/' ./test.sh)
	coverage combine ../tests/.coverage .coverage
	coverage report
