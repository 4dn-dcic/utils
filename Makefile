.PHONY: test

clear-poetry-cache:  # clear poetry/pypi cache. for user to do explicitly, never automatic
	poetry cache clear pypi --all

configure:  # does any pre-requisite installs
	pip install poetry==1.4.2

lint:
	@echo "Running flake8..."
	@flake8 dcicutils || echo "'flake8 dcicutils' failed."
	@flake8 test --exclude=data_files || echo "'flake8 test' failed."

build:  # builds
	make configure
	poetry install

test:  # runs default tests, which are the unit tests
	make test-units
	make test-static

test-for-ga:
	poetry run flake8 dcicutils
	poetry run flake8 test --exclude=data_files
	make test-units-with-coverage

retest:  # runs only failed tests from the last test run. (if no failures, it seems to run all?? -kmp 17-Dec-2020)
	poetry run pytest -vv -r w --last-failed

test-all:  # you have to be really brave to want this. a lot of things will err
	@git log -1 --decorate | head -1
	@date
	poetry run pytest -vv -r w
	@git log -1 --decorate | head -1
	@date

test-most:  # leaves out things that will probably err but runs unit tests and both kinds of integrations
	@git log -1 --decorate | head -1
	@date
	poetry run pytest -vv -r w -m "not static and not beanstalk_failure and not direct_es_query"
	@git log -1 --decorate | head -1
	@date

test-units-with-coverage:
	@git log -1 --decorate | head -1
	@date
	poetry run coverage run --source dcicutils -m pytest -vv -r w -m "not static and not integratedx and not beanstalk_failure and not direct_es_query"
	@git log -1 --decorate | head -1
	@date

test-units:  # runs unit tests (and integration tests not backed by a unit test)
	@git log -1 --decorate | head -1
	@date
	poetry run pytest -vv -r w -m "not static and not integratedx and not beanstalk_failure and not direct_es_query"
	@git log -1 --decorate | head -1
	@date

test-integrations:  # runs integration tests
	@git log -1 --decorate | head -1
	@date
	poetry run pytest -vv -r w -m "not static and (integrated or integratedx) and not beanstalk_failure and not direct_es_query"
	@git log -1 --decorate | head -1
	@date

test-direct-es-query:  # must be called inside VPC (e.g., from foursight after cloning repo, setting up venv, etc)
	@git log -1 --decorate | head -1
	@date
	poetry run pytest -vv -r w -m "direct_es_query"
	@git log -1 --decorate | head -1
	@date

test-static:
	@git log -1 --decorate | head -1
	@date
	poetry run pytest -vv -r w -m "static"
	poetry run flake8 dcicutils
	poetry run flake8 test --exclude=data_files
	@git log -1 --decorate | head -1
	@date

recordings:
	@scripts/create_test_recordings

update:  # updates dependencies
	poetry update

publish:
	# New Python based publish script (2023-04-25).
	poetry run publish-to-pypi

publish-for-ga:
	# New Python based publish script (2023-04-25).
	# For some reason, have NOT been able to get the required pip install of
	# requests and toml to "take" - when using with the poetry run publish
	# command - either here or in .main-publish.yml; still get module not
	# found error for requests in GA; so invoking directly with python.
	# poetry run publish-to-pypi --noconfirm
	python -m dcicutils.scripts.publish_to_pypi --noconfirm

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
	   $(info - Use 'make recordings' to refresh the recorded tests. (Always makes new recordings even if not needed.))
	   $(info - Use 'make clear-poetry-cache' to clear the poetry pypi cache if in a bad state. (Safe, but later recaching can be slow.))
