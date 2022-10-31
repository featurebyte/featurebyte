#* Variables
MAKE := make
EXECUTABLES = poetry git
PYLINT_DISABLE_FOR_TESTS := redefined-outer-name,invalid-name,protected-access,too-few-public-methods,unspecified-encoding,duplicate-code
POETRY_ENV_PIP := $(shell poetry env info --path)/bin/pip
PERMISSIVE_LICENSES := "\
	Public Domain;\
	MIT;\
	MIT License;\
	BSD License;\
	ISC;\
	ISC License (ISCL);\
	Python Software Foundation License;\
	Apache Software License;\
	Mozilla Public License 2.0 (MPL 2.0);\
	MPL-2.0;\
	Historical Permission Notice and Disclaimer (HPND);\
"

.PHONY: init
.PHONY: install install-databricks-sql-connector
.PHONY: update
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
install:
	poetry install -n --sync
	${MAKE} install-databricks-sql-connector

install-databricks-sql-connector:
	# databricks-sql-connector requires pyarrow = "^9.0.0" but snowflake-connector-python requires
	# pyarrow>=8.0.0,<8.1.0. Poetry does not allow this. Temporary solution is to install
	# databricks-sql-connector into the poetry managed venv using pip. databricks-sql-connector
	# works with pyarrow==8.0.0
	${POETRY_ENV_PIP} install databricks-sql-connector==2.1.0
	${POETRY_ENV_PIP} install pyarrow==8.0.0

generate-requirements-file:
	@poetry export --without-hashes > requirements.txt

#* Update
update:
	poetry update

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

lint-safety: generate-requirements-file
	@poetry run pip-licenses --packages $(shell cut -d= -f 1 requirements.txt | grep -v "\--" | tr "\n" " ") --allow-only=${PERMISSIVE_LICENSES}
	@poetry run pip-audit
	@poetry run bandit -c pyproject.toml -ll --recursive featurebyte

#* Testing
test: test-setup
	@poetry run pytest --timeout=300 --junitxml=pytest.xml -n auto --cov=featurebyte tests featurebyte | tee pytest-coverage.txt

	${MAKE} test-teardown

test-setup:
	cd .github/mongoreplicaset && ./startdb.sh

test-teardown:
	cd .github/mongoreplicaset && docker-compose down

test-routes:
	uvicorn featurebyte.app:app --reload

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
	@rm requirements.txt
