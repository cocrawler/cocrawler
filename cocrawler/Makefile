unittest:
	python -m unittest *.py plugins/*/*.py

test: unittest
	(cd ../tests; ./test.sh)

pylint:
	PYTHONPATH=. pylint *.py
