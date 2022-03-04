.PHONY: test

clear-poetry-cache:  # clear poetry/pypi cache. for user to do explicitly, never automatic
	poetry cache clear pypi --all

configure:  # does any pre-requisite installs
	pip install poetry

lint:
	flake8 dcicutils

build:  # builds
	make configure
	poetry install

test:  # runs default tests, which are the unit tests
	make test-units

retest:  # runs only failed tests from the last test run. (if no failures, it seems to run all?? -kmp 17-Dec-2020)
	poetry run pytest -vv -r w --last-failed

test-all:
	poetry run pytest -vv -r w

test-units:  # runs unit tests (and integration tests not backed by a unit test)
	@git log -1 --decorate | head -1
	@date
	poetry run pytest -vv -r w -m "not integratedx"
	@git log -1 --decorate | head -1
	@date

test-integrations:  # runs integration tests
	poetry run pytest -vv -r w -m "integrated or integratedx"

update:  # updates dependencies
	poetry update

publish:
	scripts/publish

publish-for-ga:
	scripts/publish --noconfirm

help:
	@make info

info:
	@: $(info Here are some 'make' options:)
	   $(info - Use 'make configure' to install poetry, though 'make build' will do it automatically.)
	   $(info - Use 'make lint' to check style with flake8.)
	   $(info - Use 'make build' to install dependencies using poetry.)
	   $(info - Use 'make publish' to publish this library, but only if auto-publishing has failed.)
	   $(info - Use 'make test' to run tests with the normal options we use on travis)
	   $(info - Use 'make update' to update dependencies (and the lock file))
	   $(info - Use 'make clear-poetry-cache' to clear the poetry pypi cache if in a bad state. (Safe, but later recaching can be slow.))
