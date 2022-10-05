#* Variables
MAKE := make
EXECUTABLES = poetry git
PYLINT_DISABLE_FOR_TESTS := redefined-outer-name,invalid-name,protected-access,too-few-public-methods,unspecified-encoding,duplicate-code
POETRY_ENV_PIP := $(shell poetry env info --path)/bin/pip

.PHONY: init
.PHONY: install install-nolock install-lock install-main install-dev install-lint install-docs
.PHONY: update update-main update-dev update-lint update-docs
.PHONY: format
.PHONY: lint lint-style lint-type lint-safety
.PHONY: test test-setup test-teardown
.PHONY: docs
.PHONY: clean

#* Initialize
init:
	$(foreach exec,$(EXECUTABLES),\
        $(if $(shell which $(exec)),,$(error "Missing $(exec) in $$PATH, $(exec) is required for development")))
	poetry run pre-commit install

#* Installation
install: install-lock
	${MAKE} install-nolock

install-nolock:
	${MAKE} install-main
	${MAKE} install-dev
	${MAKE} install-lint
	${MAKE} install-docs
	${MAKE} install-databricks-sql-connector

install-lock:
	poetry lock -n

install-main:
	poetry install -n --only=main

install-dev:
	poetry install -n --only=dev

install-lint:
	poetry install -n --only=lint

install-docs:
	poetry install -n --only=docs

install-databricks-sql-connector:
	${POETRY_ENV_PIP} install databricks-sql-connector==2.1.0
	${POETRY_ENV_PIP} install pyarrow==8.0.0

#* Update
update:
	${MAKE} update-main
	${MAKE} update-dev
	${MAKE} update-lint
	${MAKE} update-docs

update-main:
	poetry update --only=main

update-dev:
	poetry update --only=dev

update-lint:
	poetry update --only=lint

update-docs:
	poetry update --only=docs

#* Formatters
format:
	@poetry run pyupgrade --py38-plus **/*.py
	@poetry run isort .
	@poetry run black .
	@poetry run toml-sort pyproject.toml --all --in-place

#* Linting
lint: lint-style lint-type lint-safety

lint-style:
	@poetry run isort --diff --check-only --settings-path pyproject.toml .
	@poetry run black --diff --check .
	@poetry run pylint --rcfile pyproject.toml featurebyte
	@poetry run pylint --disable=${PYLINT_DISABLE_FOR_TESTS} --rcfile pyproject.toml tests

	@find featurebyte -type d \( -path featurebyte/routes \) -prune -false -o -name "*.py" | xargs poetry run darglint --verbosity 2
	@find featurebyte -type f \( -path featurebyte/routes \) -o -name "controller.py" | xargs poetry run darglint --verbosity 2

lint-type:
	@poetry run mypy --install-types --non-interactive --config-file pyproject.toml .

lint-safety:
	@poetry run pip-audit
	@poetry run bandit -c pyproject.toml -ll --recursive featurebyte

#* Testing
test: test-setup
	@poetry run coverage run -m pytest -c pyproject.toml --timeout=180 --junitxml=pytest.xml tests featurebyte | tee pytest-coverage.txt
	# Hack to support github-coverage action
	@echo "coverage: platform" >> pytest-coverage.txt

	@poetry run coverage report -m | tee -a pytest-coverage.txt
	${MAKE} test-teardown

test-setup:
	cd .github/mongoreplicaset && ./startdb.sh

test-teardown:
	cd .github/mongoreplicaset && docker-compose down

test-routes:
	uvicorn featurebyte.app:app --reload

#* Docs Generation
docs:
	poetry run sphinx-build -b html docs/source docs/build

#* Cleaning
clean:
	@echo "Running Clean"
	@find . | grep -E "./.coverage*" | xargs rm -rf || true
	@find . | grep -E "(__pycache__|\.pyc|\.pyo$$)" | xargs rm -rf || true
	@find . | grep -E ".DS_Store" | xargs rm -rf || true
	@find . | grep -E ".mypy_cache" | xargs rm -rf || true
	@find . | grep -E ".ipynb_checkpoints" | xargs rm -rf || true
	@find . | grep -E ".pytest_cache" | xargs rm -rf|| true
	@rm pytest-coverage.txt || true
	@rm pytest.xml || true
	@rm -rf dist/ docs/build htmlcov/ || true
