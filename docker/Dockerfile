FROM python:3.8-slim

ARG USER=fb_user
ARG UID=1000

ARG FEATUREBYTE_NP_PASSWORD
ARG FEATUREBYTE_REPOSITORY=us-central1-python.pkg.dev/vpc-host-nonprod-xa739-xz970/featurebyte-pypi
ARG FEATUREBYTE_NP_USERNAME=_json_key_base64
ENV FEATUREBYTE_REPOSITORY=${FEATUREBYTE_REPOSITORY?missing_repository}
ENV POETRY_HTTP_BASIC_FEATUREBYTE_NP_USERNAME=${FEATUREBYTE_NP_USERNAME?missing_username}
ENV POETRY_HTTP_BASIC_FEATUREBYTE_NP_PASSWORD=${FEATUREBYTE_NP_PASSWORD?missing_featurebyte_serviceaccount_base64_key}

# install build tools
RUN apt-get update && \
    apt-get install -y build-essential netcat curl && \
    apt-get clean

# install poetry
RUN pip install poetry==1.3.1

# Copy project files
WORKDIR /app
COPY poetry.lock pyproject.toml /app/

# Install requirements
RUN poetry config virtualenvs.create false && \
    poetry install --only main --only docs --extras "server" --no-interaction --no-ansi && \
    poetry config http-basic.featurebyte_np ${POETRY_HTTP_BASIC_FEATUREBYTE_NP_USERNAME} ${POETRY_HTTP_BASIC_FEATUREBYTE_NP_PASSWORD} && \
    rm -rf ~/.cache/pypoetry/artifacts && \
    rm -rf ~/.cache/pypoetry/cache

# copy featurebyte
COPY ./dist            /app/dist

RUN pip install jupyterlab && \
    pip install --no-index -f /app/dist featurebyte && \
    unset POETRY_HTTP_BASIC_FEATUREBYTE_NP_PASSWORD && \
    unset POETRY_HTTP_BASIC_FEATUREBYTE_NP_USERNAME && \
    poetry config --unset http-basic.featurebyte_np

# Copy scripts
COPY ./docker/entrypoint-featurebyte.sh /scripts/entrypoint.sh
COPY ./docker/migration.py              /scripts/migration.py

RUN adduser --uid $UID --disabled-password --gecos '' $USER
RUN chown -R $USER:$USER /app

USER $USER

ENTRYPOINT ["/bin/bash", "/scripts/entrypoint.sh"]
