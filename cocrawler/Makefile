pytest:
	py.test

test: pytest
	(cd ../tests; ./test.sh)

pylint:
	PYTHONPATH=. pylint *.py

clean_coverage:
	rm -f .coverage
	rm -f .coverage.*

test_coverage: clean_coverage
	PYTHONDONTWRITEBYTECODE=1 py.test --cov-report= --cov-append --cov stats --cov cocrawler --cov crawl --cov config --cov datalayer --cov fetcher --cov parse --cov robots --cov seeds --cov stats *.py -v
	(cd ../tests; COVERAGE='coverage run --source=../cocrawler/' ./test.sh)
	coverage combine ../tests/.coverage .coverage
	coverage report
