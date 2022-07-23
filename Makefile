#* Variables
MAKE := make

.PHONY: init
.PHONY: install install-lock install-main install-dev install-lint install-docs
.PHONY: update update-main update-dev update-lint update-docs
.PHONY: format
.PHONY: lint lint-style lint-type lint-safety
.PHONY: test
.PHONY: docs
.PHONY: clean

#* Initialize
init:
	poetry run pre-commit install
	# TODO: Add kubectl, helm, poetry checks here

#* Installation
install: install-lock
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
	@poetry run pylint --rcfile pyproject.toml featurebyte --exit-zero

	# TODO: Add documentation linter
	# @find featurebyte -type f -prune -false -o -name "*.py" | xargs poetry run darglint --docstring-style numpy --strictness full -v 2

lint-type:
	@poetry run mypy --install-types --non-interactive --config-file pyproject.toml .

lint-safety:
	@poetry run safety check --short-report
	@poetry run bandit -c pyproject.toml -ll --recursive featurebyte

#* Testing
test:
	@poetry run coverage run -m pytest -c pyproject.toml --timeout=120 --junitxml=pytest.xml tests featurebyte 2>/dev/null -q | tee pytest-coverage.txt
	@poetry run coverage-badge -o assets/images/coverage.svg -f -q    # Write coverage badge
	@poetry run coverage report -m | tee -a pytest-coverage.txt

#* Docs Generation
docs:
	poetry run sphinx-build -b html docs/source docs/build

#* Cleaning
clean:
	@echo "Running Clean"
	@find . | grep -E "./.coverage*" | xargs rm -rf									|| true
	@find . | grep -E "(__pycache__|\.pyc|\.pyo$$)" | xargs rm -rf 	|| true
	@find . | grep -E ".DS_Store" | xargs rm -rf										|| true
	@find . | grep -E ".mypy_cache" | xargs rm -rf									|| true
	@find . | grep -E ".ipynb_checkpoints" | xargs rm -rf						|| true
	@find . | grep -E ".pytest_cache" | xargs rm -rf								|| true
	@rm -rf pytest-coverage.txt																			|| true
	@rm -rf dist/ docs/build htmlcov/																|| true
