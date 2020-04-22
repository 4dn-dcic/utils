.PHONY: test

configure:  # does any pre-requisite installs
	pip install poetry

lint:
	flake8 dcicutils

build:  # builds
	make configure
	poetry install

test:
	pytest -vv

info:
	@: $(info Here are some 'make' options:)
	   $(info - Use 'make configure' to install poetry, though 'make build' will do it automatically.)
	   $(info - Use 'make lint' to check style with flake8.)
	   $(info - Use 'make build' to install dependencies using poetry.)
	   $(info - Use 'make test' to run tests with the normal options we use on travis)
