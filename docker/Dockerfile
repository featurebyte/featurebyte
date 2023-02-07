FROM python:3.8-slim AS builder

ARG FEATUREBYTE_NP_PASSWORD
ARG FEATUREBYTE_REPOSITORY=us-central1-python.pkg.dev/vpc-host-nonprod-xa739-xz970/featurebyte-pypi
ARG FEATUREBYTE_NP_USERNAME=_json_key_base64
ENV FEATUREBYTE_REPOSITORY=${FEATUREBYTE_REPOSITORY?missing_repository}
ENV POETRY_HTTP_BASIC_FEATUREBYTE_NP_USERNAME=${FEATUREBYTE_NP_USERNAME?missing_username}
ENV POETRY_HTTP_BASIC_FEATUREBYTE_NP_PASSWORD=${FEATUREBYTE_NP_PASSWORD?missing_featurebyte_serviceaccount_base64_key}

# install build tools
RUN apt-get update && \
    apt-get install -y build-essential curl wkhtmltopdf && \
    apt-get clean

# install poetry
RUN pip install poetry==1.3.1

WORKDIR /app
COPY poetry.lock pyproject.toml /app/

# Install into .venv
RUN poetry config virtualenvs.in-project true && \
    poetry install --only main --extras "server" --no-interaction --no-ansi && \
    poetry run pip install jupyterlab==3.5.3

FROM python:3.8-slim AS runner

ARG UID=1000
ARG USER=fb_user
ARG USER_GROUP=users
ARG USER_HOME=/app

RUN useradd -m -r -d $USER_HOME -g users -u $UID $USER

RUN apt-get update && \
    apt-get install -y netcat && \
    apt-get clean

## copy featurebyte
WORKDIR $USER_HOME
USER $USER

## Copy binaries and build
COPY --chown=$USER:$USER_GROUP --from=builder /app/.venv $USER_HOME/.venv
COPY --chown=$USER:$USER_GROUP ./dist $USER_HOME/dist
ENV PATH="${USER_HOME}/.venv/bin:${PATH}"
RUN bash -c "source ${USER_HOME}/.venv/bin/activate; pip install --no-index -f ${USER_HOME}/dist featurebyte"  # Install featurebyte into /app/.venv

## Copy scripts
COPY ./docker/entrypoint.sh             /scripts/entrypoint.sh
COPY ./docker/samples/                  /samples

CMD ["/bin/bash", "/scripts/entrypoint.sh"]
