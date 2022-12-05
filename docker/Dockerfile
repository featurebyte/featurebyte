# Need to route to different cpu arch
FROM python:3.8-slim

ARG FEATUREBYTE_NP_PASSWORD
ARG FEATUREBYTE_REPOSITORY=us-central1-python.pkg.dev/vpc-host-nonprod-xa739-xz970/featurebyte-pypi
ARG FEATUREBYTE_NP_USERNAME=_json_key_base64
ENV FEATUREBYTE_REPOSITORY=${FEATUREBYTE_REPOSITORY?missing_repository}
ENV POETRY_HTTP_BASIC_FEATUREBYTE_NP_USERNAME=${FEATUREBYTE_NP_USERNAME?missing_username}
ENV POETRY_HTTP_BASIC_FEATUREBYTE_NP_PASSWORD=${FEATUREBYTE_NP_PASSWORD?missing_featurebyte_serviceaccount_base64_key}

# install build tools
RUN apt-get update && \
    apt-get install -y build-essential netcat && \
    apt-get clean

# install poetry
RUN pip install --extra-index-url "https://${POETRY_HTTP_BASIC_FEATUREBYTE_NP_USERNAME}:${POETRY_HTTP_BASIC_FEATUREBYTE_NP_PASSWORD}@${FEATUREBYTE_REPOSITORY}/simple/" poetry==1.2.0rc99

# Copy project files
WORKDIR /app
COPY poetry.lock pyproject.toml /app/
COPY ./docker/entrypoint-featurebyte.sh /scripts/entrypoint.sh
COPY ./docker/migration.py              /scripts/migration.py

# Install requirements
RUN poetry config virtualenvs.create false && \
    poetry install --only main --only docs --extras "server" --no-interaction --no-ansi && \
    rm -rf ~/.cache/pypoetry/artifacts && \
    rm -rf ~/.cache/pypoetry/cache

# copy featurebyte and featurebyte package
COPY ./featurebyte /app/featurebyte
COPY ./dist        /app/dist
COPY ./site        /app/site

# install databricks-connector via pip (dependency error with pyarrow)
# downgrade pyarrow - backwards compatibility issue with databricks
RUN pip install databricks-sql-connector==2.1.0 && \
    pip install pyarrow==8.0.0 && \
    pip install jupyterlab && \
    pip install --no-index -f /app/dist featurebyte && \
    unset POETRY_HTTP_BASIC_FEATUREBYTE_NP_PASSWORD

# Install nginx
RUN apt-get update -y &&\
    apt-get install nginx -y &&\
    apt-get clean

RUN sed -i 's|root /var/www/html;|root /app/site;|g' /etc/nginx/sites-available/default

ENTRYPOINT ["/bin/bash", "/scripts/entrypoint.sh"]
