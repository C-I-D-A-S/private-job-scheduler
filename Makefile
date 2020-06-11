PKG = scheduler
VERSION=$(shell awk '{match($$0,"__version__ = '\''(.*)'\''",a)}END{print a[1]}' $(PKG)/__version__.py)

.PHONY: version init flake8 pylint lint test coverage clean

version:
	@echo $(VERSION)

clean:
	find . -type f -name '*.py[co]' -delete
	find . -type d -name '__pycache__' -delete
	rm -rf dist
	rm -rf build
	rm -rf *.egg-info
	rm -rf .hypothesis
	rm -rf .pytest_cache
	rm -rf .tox
	rm -f report.xml
	rm -f coverage.xml

init: clean
	pipenv --python 3.7
	pipenv install --dev

lint: pylint flake8

flake8:
	pipenv run flake8 scheduler --max-line-length=120

pylint:
	pipenv run pylint scheduler --ignore=tests

black:
	pipenv run black $(PKG)

mypy:
	pipenv run mypy $(PKG)

test:
	pipenv run pytest --pep8


coverage:
	pipenv run pytest --cov-report term-missing --cov-report xml --cov=$(PKG) udc_api/tests


build: clean build-cython clean-modules

build-cython:
	cp Pipfile Pipfile.lock cython/
	cp -r scheduler cython/
	docker build -t scheduler cython/ --no-cache

clean-modules:
	rm -f cython/Pipfile*
	rm -rf cython/scheduler
