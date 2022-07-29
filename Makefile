#* Variables
MAKE := make
EXECUTABLES = poetry git

.PHONY: init
.PHONY: install install-nolock install-lock install-main install-dev install-lint install-docs
.PHONY: update update-main update-dev update-lint update-docs
.PHONY: format
.PHONY: lint lint-style lint-type lint-safety
.PHONY: test
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

	@find featurebyte -type d \( -path featurebyte/routes \) -prune -false -o -name "*.py" | xargs poetry run darglint --verbosity 2
	@find featurebyte -type f \( -path featurebyte/routes \) -o -name "controller.py" | xargs poetry run darglint --verbosity 2

lint-type:
	@poetry run mypy --install-types --non-interactive --config-file pyproject.toml .

lint-safety:
	@poetry run safety check --short-report
	@poetry run bandit -c pyproject.toml -ll --recursive featurebyte

#* Testing
test:
	@poetry run coverage run -m pytest -c pyproject.toml --timeout=120 --junitxml=pytest.xml tests featurebyte | tee pytest-coverage.txt
	# Hack to support github-coverage action
	@echo "coverage: platform" >> pytest-coverage.txt

	@poetry run coverage report -m | tee -a pytest-coverage.txt

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
