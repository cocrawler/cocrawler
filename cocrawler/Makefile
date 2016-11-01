pytest:
	tldextract -u -p
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
	tldextract -u -p
	py.test --cov-report= --cov-append --cov stats --cov cocrawler --cov crawl --cov conf --cov datalayer --cov fetcher --cov parse --cov robots --cov seeds --cov stats --cov url_allowed --cov useragent --cov accumulator --cov urls --cov topk --cov burner --cov dns --cov webserver --cov timer test_*.py -v
	coverage run -a --source=. crawl.py --printdefault | wc -l | awk '{ if( $$1 > 10) {exit 0;} else {exit 1;} }'
	(cd ../tests; COVERAGE='coverage run -a --source=../cocrawler/' ./test.sh)
	coverage combine ../tests/.coverage .coverage
	coverage report

run_parsers:
	python ./run_parsers.py ~/public_html/
