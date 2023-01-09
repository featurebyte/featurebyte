#* Variables
MAKE := make
EXECUTABLES = poetry docker
PYLINT_DISABLE := too-few-public-methods
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
	Apache License 2.0;\
	Mozilla Public License 2.0 (MPL 2.0);\
	MPL-2.0;\
	Historical Permission Notice and Disclaimer (HPND);\
"

.PHONY: init
.PHONY: install install-databricks-sql-connector
.PHONY: format
.PHONY: lint lint-style lint-type lint-safety lint-requirements-txt
.PHONY: test test-setup test-teardown
.PHONY: docs docs-build
.PHONY: beta-start beta-bundle beta-stop beta-build beta-bundle-publish
.PHONY: clean

#* Initialize
init: install
	$(foreach exec,$(EXECUTABLES),\
        $(if $(shell which $(exec)),,$(error "Missing $(exec) in $$PATH, $(exec) is required for development")))
	poetry run pre-commit install

#* Installation
install:
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

	find featurebyte -type d \( -path featurebyte/routes \) -prune -false -o -name "*.py" | xargs poetry run darglint --verbosity 2
	find featurebyte -type f \( -path featurebyte/routes \) -o -name "controller.py" | xargs poetry run darglint --verbosity 2

lint-type:
	poetry run mypy --install-types --non-interactive --config-file pyproject.toml .

lint-requirements-txt:
	poetry export --without-hashes > requirements.txt

lint-safety: | lint-requirements-txt
	# Exporting dependencies to requirements.txt
	poetry run pip-licenses --packages $(shell cut -d= -f 1 requirements.txt | grep -v "\--" | tr "\n" " ") --allow-only=${PERMISSIVE_LICENSES}
	poetry run pip-audit --ignore-vul GHSA-hcpj-qp55-gfph

	poetry run bandit -c pyproject.toml -ll --recursive featurebyte
#* Testing
test: test-setup
	poetry run pytest --timeout=240 --junitxml=pytest.xml -n auto --cov=featurebyte tests featurebyte | tee pytest-coverage.txt
	${MAKE} test-teardown

test-setup:
	cd .github/mongoreplicaset && docker compose up -d

test-teardown:
	cd .github/mongoreplicaset && docker compose down

test-routes:
	uvicorn featurebyte.app:app --reload

#* Docker
beta-start: beta-build
	cd docker && docker compose up

beta-stop:
	cd docker && docker compose down

beta-build:
	poetry build   # We are exporting dist/ to the image
	docker buildx build -f docker/Dockerfile -t "featurebyte-beta:latest" --build-arg FEATUREBYTE_NP_PASSWORD="$$FEATUREBYTE_NP_PASSWORD" .

beta-bundle:
	-mkdir beta
	# Copy dependencies over to bundled folder
	cp docker/entrypoint-mongo.sh      beta/entrypoint-mongo.sh
	cp docker/docker-compose-beta.yml  beta/docker-compose.yml
	cp docker/start.py                 beta/start.py

	@test -s beta/creds.json.b64 || echo "Credential file is missing at beta/creds.json.b64"
	@test -s beta/creds.json.b64 || echo 'create the file with contents: $$(echo "_json_base_64:$$(cat SERVICE_ACCOUNT_FILE | base64)" | base64)'
	@test -s beta/creds.json.b64 || exit 1

	# Compress with tar.gz and zip
	tar czvf featurebyte_beta.tar.gz beta/
	zip -9 featurebyte_beta.zip -r beta/

#* Docs Generation
DOCS_CMD := PYTHONPATH=$(PWD)/docs/extensions FB_GENERATE_FULL_DOCS=1 poetry run mike
docs:
	${MAKE} docs-build
	${MAKE} docs-serve

docs-serve:
	${DOCS_CMD} serve --config-file mkdocs.yaml

# This will automatically tag the version as poetry version (0.1.33) => (0.1)
# And commit it to your local git
# Do not push to origin, this will be done via a github action
docs-build:
	${DOCS_CMD} deploy --config-file mkdocs.yaml --update-aliases $(shell poetry version -s | grep -oP '^[0-9]+[.][0-9]+') latest

docs-dev:
	PYTHONPATH=$(PWD)/docs/extensions poetry run mkdocs serve --no-livereload

#* Cleaning
clean:
	git stash -u
	git clean -dfx --exclude=.idea/
	git stash pop
