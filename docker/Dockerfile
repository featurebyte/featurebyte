FROM python:3.8-slim AS builder

ARG FEATUREBYTE_NP_PASSWORD
ARG FEATUREBYTE_REPOSITORY=us-central1-python.pkg.dev/vpc-host-nonprod-xa739-xz970/featurebyte-pypi
ARG FEATUREBYTE_NP_USERNAME=_json_key_base64
ENV FEATUREBYTE_REPOSITORY=${FEATUREBYTE_REPOSITORY?missing_repository}
ENV POETRY_HTTP_BASIC_FEATUREBYTE_NP_USERNAME=${FEATUREBYTE_NP_USERNAME?missing_username}
ENV POETRY_HTTP_BASIC_FEATUREBYTE_NP_PASSWORD=${FEATUREBYTE_NP_PASSWORD?missing_featurebyte_serviceaccount_base64_key}

# install build tools
RUN apt-get update && \
    apt-get install -y build-essential curl libsasl2-dev && \
    apt-get clean

# install poetry
RUN pip install poetry==1.3.1

WORKDIR /app
COPY poetry.lock pyproject.toml /app/

# Install into .venv
RUN poetry config virtualenvs.in-project true && \
    poetry install --only main --extras "server" --no-interaction --no-ansi

FROM python:3.8-slim AS runner

ARG USER_HOME=/app

RUN apt-get update && \
    apt-get install -y netcat libsasl2-dev wkhtmltopdf gosu curl && \
    apt-get clean

WORKDIR $USER_HOME
COPY --from=builder /app/.venv $USER_HOME/.venv
COPY ./dist $USER_HOME/dist
ENV PATH="${USER_HOME}/.venv/bin:${PATH}"
RUN bash -c "source ${USER_HOME}/.venv/bin/activate; pip install --no-index -f ${USER_HOME}/dist featurebyte"  # Install featurebyte into /app/.venv

## Copy script
COPY ./docker/entrypoint.sh             /scripts/entrypoint.sh
RUN chmod +x /scripts/entrypoint.sh

CMD ["/bin/bash", "/scripts/entrypoint.sh"]
