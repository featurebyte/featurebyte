FROM python:3.8-slim AS builder

ARG FEATUREBYTE_NP_PASSWORD
ARG FEATUREBYTE_REPOSITORY=us-central1-python.pkg.dev/vpc-host-nonprod-xa739-xz970/featurebyte-pypi
ARG FEATUREBYTE_NP_USERNAME=_json_key_base64
ENV FEATUREBYTE_REPOSITORY=${FEATUREBYTE_REPOSITORY?missing_repository}
ENV POETRY_HTTP_BASIC_FEATUREBYTE_NP_USERNAME=${FEATUREBYTE_NP_USERNAME?missing_username}
ENV POETRY_HTTP_BASIC_FEATUREBYTE_NP_PASSWORD=${FEATUREBYTE_NP_PASSWORD?missing_featurebyte_serviceaccount_base64_key}

ARG UID=1000
ARG USER=fb_user
ARG USER_GROUP=users
ARG USER_HOME=/app

# install build tools
RUN apt-get update && \
    apt-get install -y build-essential netcat curl wkhtmltopdf && \
    apt-get clean

# install poetry
RUN pip install poetry==1.3.1
RUN useradd -m -r -d $USER_HOME -g users -u $UID $USER


# Copy project files
WORKDIR $USER_HOME
USER $USER
COPY --chown=$USER:$USER_GROUP poetry.lock pyproject.toml /app/

# Install into .venv
RUN poetry config virtualenvs.in-project true && \
    poetry install --only main --only docs --extras "server" --no-interaction --no-ansi  && \
    poetry add jupyterlab==3.5.3

FROM python:3.8-slim AS runner

ARG USER=fb_user
ARG UID=1000

ARG UID=1000
ARG USER=fb_user
ARG USER_GROUP=users
ARG USER_HOME=/app

RUN useradd -m -r -d $USER_HOME -g users -u $UID $USER

## copy featurebyte
WORKDIR $USER_HOME
USER $USER

## Copy binaries and build
COPY --chown=$USER:$USER_GROUP --from=builder /app/.venv /app/.venv

## Copy scripts
COPY ./docker/entrypoint.sh             /scripts/entrypoint.sh
COPY ./docker/samples/                  /samples

CMD ["/bin/bash", "/scripts/entrypoint.sh"]
