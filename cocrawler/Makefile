unittest:
	py.test

test: unittest
	(cd ../tests; ./test.sh)

pylint:
	PYTHONPATH=. pylint *.py
