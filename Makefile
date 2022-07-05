#* Variables
SHELL := /usr/bin/env bash
PYTHON := python
PYTHONPATH := `pwd`

#* Poetry
.PHONY: poetry-download
poetry-download:
	curl -sSL https://install.python-poetry.org | $(PYTHON) -

.PHONY: poetry-remove
poetry-remove:
	curl -sSL https://install.python-poetry.org | $(PYTHON) - --uninstall

#* Installation
.PHONY: install
install:
	poetry lock -n
	poetry install -n
	-poetry run mypy --config-file pyproject.toml --install-types --non-interactive ./

.PHONY: pre-commit-install
pre-commit-install:
	poetry run pre-commit install

#* Formatters
.PHONY: codestyle
codestyle:
	poetry run pyupgrade --exit-zero-even-if-changed --py37-plus **/*.py
	poetry run isort --settings-path pyproject.toml ./
	poetry run black --config pyproject.toml ./

.PHONY: formatting
formatting: codestyle

#* Linting
.PHONY: test
test:
	PYTHONPATH=$(PYTHONPATH) poetry run pytest --junitxml=pytest.xml --cov-report=term-missing --cov=featurebyte tests/ featurebyte/ | tee pytest-coverage.txt
	poetry run coverage-badge -o assets/images/coverage.svg -f

.PHONY: check-codestyle
check-codestyle:
	poetry run isort --diff --check-only --settings-path pyproject.toml ./
	poetry run black --diff --check --config pyproject.toml ./
	find featurebyte tests -iname "*.py" | xargs poetry run pylint --rcfile=.pylintrc
	find featurebyte -type d \( -path featurebyte/routes \) -prune -false -o -name "*.py" | xargs poetry run darglint --verbosity 2

.PHONY: mypy
mypy:
	poetry run mypy --config-file pyproject.toml ./

.PHONY: check-safety
check-safety:
	poetry check
	poetry run safety check --full-report
	poetry run bandit -ll --recursive featurebyte

.PHONY: lint
lint: test check-codestyle mypy check-safety

.PHONY: update-dev-deps
update-dev-deps:
	poetry add -D bandit@latest darglint@latest "isort[colors]@latest" mypy@latest pre-commit@latest pylint@latest pytest@latest pyupgrade@latest safety@latest coverage@latest coverage-badge@latest pytest-html@latest pytest-cov@latest
	poetry add -D --allow-prereleases black@latest

#* Cleaning
.PHONY: pycache-remove
pycache-remove:
	find . | grep -E "(__pycache__|\.pyc|\.pyo$$)" | xargs rm -rf

.PHONY: dsstore-remove
dsstore-remove:
	find . | grep -E ".DS_Store" | xargs rm -rf

.PHONY: mypycache-remove
mypycache-remove:
	find . | grep -E ".mypy_cache" | xargs rm -rf

.PHONY: ipynbcheckpoints-remove
ipynbcheckpoints-remove:
	find . | grep -E ".ipynb_checkpoints" | xargs rm -rf

.PHONY: pytestcache-remove
pytestcache-remove:
	find . | grep -E ".pytest_cache" | xargs rm -rf

.PHONY: build-remove
build-remove:
	rm -rf dist docs/build

.PHONY: cleanup
cleanup: pycache-remove dsstore-remove mypycache-remove ipynbcheckpoints-remove pytestcache-remove

# Build Artifacts
build-artifacts:
	poetry build

# Docs Generation
build-docs: build-remove
	poetry install -E docs
	poetry run sphinx-build -b html docs/source docs/build
