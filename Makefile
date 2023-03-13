#* Variables
MAKE := make
PYLINT_DISABLE := too-few-public-methods
PYLINT_DISABLE_FOR_TESTS := redefined-outer-name,invalid-name,protected-access,too-few-public-methods,unspecified-encoding,duplicate-code
PERMISSIVE_LICENSES := "\
	Apache License 2.0;\
	Apache License, Version 2.0;\
	Apache Software License;\
	BSD;\
	BSD License;\
	Historical Permission Notice and Disclaimer (HPND);\
	GNU General Public License v2 (GPLv2);\
	ISC License (ISCL);\
	ISC;\
	MIT License;\
	MIT;\
	MPL-2.0;\
	Mozilla Public License 2.0 (MPL 2.0);\
	Public Domain;\
	Python Software Foundation License;\
	The Unlicense (Unlicense);\
"

.PHONY: init
.PHONY: install
.PHONY: format
.PHONY: lint lint-style lint-type lint-safety lint-requirements-txt
.PHONY: test test-setup test-teardown
.PHONY: docs docs-build
.PHONY: docker-build
.PHONY: clean

#* Initialize
init: install
	poetry run pre-commit install

#* Installation
build-hive-udf-jar:
	cd hive-udf && ./gradlew test && ./gradlew shadowJar
	rm -f featurebyte/sql/spark/*.jar
	cp hive-udf/lib/build/libs/*.jar featurebyte/sql/spark/

install: build-hive-udf-jar
	poetry install -n --sync --extras=server

#* Formatters
format:
	poetry run pyupgrade --py38-plus **/*.py
	poetry run isort .
	poetry run black .
	poetry run toml-sort --all --in-place pyproject.toml poetry.lock

#* Linting
lint: lint-style lint-type lint-safety

lint-style:
	poetry run toml-sort --check poetry.lock pyproject.toml    # Check if user been using pre-commit hook
	poetry run isort --diff --check-only --settings-path pyproject.toml .
	poetry run black --diff --check .
	poetry run pylint --disable=${PYLINT_DISABLE} --rcfile pyproject.toml featurebyte
	poetry run pylint --disable=${PYLINT_DISABLE_FOR_TESTS} --rcfile pyproject.toml tests

	find featurebyte -type d \( -path featurebyte/routes \) -prune -false -o -name "*.py" ! -path "featurebyte/__main__.py" ! -path "featurebyte/datasets/*" | xargs poetry run darglint --verbosity 2
	find featurebyte -type f \( -path featurebyte/routes \) -o -name "controller.py" | xargs poetry run darglint --verbosity 2

lint-type:
	poetry run mypy --install-types --non-interactive --config-file pyproject.toml .

lint-safety:
	poetry run pip-licenses --packages $(shell poetry export --without-hashes --without-urls --extras server | cut -d '=' -f1 | xargs) --allow-only=${PERMISSIVE_LICENSES}
	poetry run pip-audit --ignore-vul GHSA-w7pp-m8wf-vj6r --ignore-vul GHSA-x4qr-2fvf-3mr5 --ignore-vul GHSA-74m5-2c7w-9w3x
	poetry run bandit -c pyproject.toml -ll --recursive featurebyte

#* Testing
test:
	${MAKE} build-hive-udf-jar
	${MAKE} test-setup
	${MAKE} test-unit
	${MAKE} test-integration-all
	${MAKE} test-merge
	${MAKE} test-teardown

test-unit:
	poetry run pytest --timeout=240 --junitxml=pytest.xml.0 -n auto --cov=featurebyte tests/unit

test-integration-all:
	${MAKE} test-integration-snowflake
	${MAKE} test-integration-spark

test-integration-snowflake:
	poetry run pytest --timeout=240 --junitxml=pytest.xml.1 -n auto --cov=featurebyte tests/integration --source-types none,snowflake

test-integration-spark:
	poetry run pytest --timeout=240 --junitxml=pytest.xml.2 --cov=featurebyte tests/integration --source-types spark --maxfail=1

test-docs:
	poetry run pytest --timeout=240 featurebyte

test-merge:
	echo "coverage: platform" > pytest-coverage.txt
	poetry run coverage combine
	poetry run coverage report >> pytest-coverage.txt
	poetry run junitparser merge pytest.xml.* pytest.xml

test-setup:
	bash scripts/test-setup.sh

test-teardown:
	cd docker/test && docker compose down

test-routes:
	uvicorn featurebyte.app:app --reload

docker-build: | build-hive-udf-jar
	poetry build
	docker buildx build . -f docker/Dockerfile --build-arg FEATUREBYTE_NP_PASSWORD="$$FEATUREBYTE_NP_PASSWORD" -t featurebyte-server:latest

docker-dev: | docker-build
	poetry run featurebyte start --local
	poetry run featurebyte start spark --local

docker-dev-stop:
	poetry run featurebyte stop
	poetry run featurebyte stop spark

#* Cleaning
clean:
	git stash -u
	git clean -dfx --exclude=.idea/
	git stash pop

#* Api documentation
docs:
	PYTHONPATH=$(PWD)/docs/extensions FB_GENERATE_FULL_DOCS=1 poetry run mkdocs serve --config-file mkdocs.yaml --no-livereload
