unittest:
	python -m unittest *.py plugins/*/*.py

test: unittest
	(cd ../test; ./test.sh)

pylint:
	PYTHONPATH=. pylint *.py
