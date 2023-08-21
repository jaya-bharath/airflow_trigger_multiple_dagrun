PYLINT_SCORE := 9.9
PYTEST_COVERAGE_SCORE := 100
PYTEST_FOLDER := tests/
PACKAGE_NAME := airflow_trigger_multiple_dagrun

default: help

## Display this help text
help:
	$(info Available targets)
	@awk '/^[a-zA-Z\-\_0-9]+:/ {                    \
          nb = sub( /^## /, "", helpMsg );              \
          if(nb == 0) {                                 \
            helpMsg = $$0;                              \
            nb = sub( /^[^:]*:.* ## /, "", helpMsg );   \
          }                                             \
          if (nb)                                       \
            print  $$1 helpMsg;                         \
        }                                               \
        { helpMsg = $$0 }'                              \
        $(MAKEFILE_LIST) | sort | column -ts:

## install poetry dependency
install:
	@poetry install --no-interaction --no-ansi --remove-untracked

## clean the temp
clean:
	@find . -name *_cache -exec rm -rf {} +
	@find . -name htmlcov -exec rm -rf {} +
	@find . -name ".coverage*" -exec rm -rf '{}' +

## run unit tests
unit_tests:
	@poetry run coverage erase
	@poetry run py.test --cov=$(PACKAGE_NAME) --cov-fail-under=$(PYTEST_COVERAGE_SCORE) $(PYTEST_FOLDER)

test: unit_tests lint clean ## run test lint

## code lint check
lint:
	@poetry run pylint --recursive y --fail-under=$(PYLINT_SCORE) .


## Run setup
setup:
	@poetry install --no-interaction --no-ansi --sync
	@poetry config repositories.testpypi https://test.pypi.org/legacy/


.phony: help install clean unit_tests test lint help setup
