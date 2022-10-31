FROM python:3.8-slim

ARG USER=user
ARG UID=1000

# FEATUREBYTE_NP_PASSWORD == base64 encoded serviceaccount key
ARG FEATUREBYTE_NP_PASSWORD
ARG FEATUREBYTE_REPOSITORY=us-central1-python.pkg.dev/vpc-host-nonprod-xa739-xz970/featurebyte-pypi
ARG FEATUREBYTE_NP_USERNAME=_json_key_base64
ENV FEATUREBYTE_REPOSITORY=${FEATUREBYTE_REPOSITORY?missing_repository}
ENV POETRY_HTTP_BASIC_FEATUREBYTE_NP_USERNAME=${FEATUREBYTE_NP_USERNAME?missing_username}
ENV POETRY_HTTP_BASIC_FEATUREBYTE_NP_PASSWORD=${FEATUREBYTE_NP_PASSWORD?missing_featurebyte_serviceaccount_base64_key}

# install build tools
RUN apt-get update && \
    apt-get install -y build-essential git dnsutils wget && \
    apt-get clean

# install mongodb
RUN wget -qO - https://www.mongodb.org/static/pgp/server-5.0.asc | apt-key add -
RUN echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-5.0.list
RUN apt-get update && \
    apt install -y software-properties-common gnupg apt-transport-https ca-certificates && \
    apt install -y mongodb-org && \
    apt-get clean

RUN mkdir -p ~/.config/pip && printf "%s\n" "[global]" "extra-index-url = https://${POETRY_HTTP_BASIC_FEATUREBYTE_NP_USERNAME}:${POETRY_HTTP_BASIC_FEATUREBYTE_NP_PASSWORD}@${FEATUREBYTE_REPOSITORY}/simple/" > ~/.config/pip/pip.conf && \
    pip install poetry==1.2.0rc99 --no-cache-dir && \
    rm -rf ~/.config/pip

# Extend PATH to use pip-installed tools from shell (like alembic)
ENV PATH="${PATH}:/home/$USER/.local/bin"

# Add non-root user
RUN adduser --uid $UID --disabled-password --gecos '' $USER

# Install requirements
COPY poetry.lock pyproject.toml ./
RUN poetry config virtualenvs.create false && \
    poetry install --only main,server --no-interaction --no-ansi && \
    rm -rf ~/.cache/pypoetry/artifacts && \
    rm -rf ~/.cache/pypoetry/cache
RUN pip install --no-cache-dir databricks-sql-connector==2.1.0 && \
    pip install --no-cache-dir pyarrow==8.0.0

# Copy project files
COPY ./docker/entrypoint.sh ./docker/migration.py /scripts/
COPY ./featurebyte /home/$USER/app/featurebyte
RUN chown "$USER:$USER" -R /home/$USER/app

USER $USER
WORKDIR /home/$USER/app

ENTRYPOINT ["/bin/sh", "/scripts/entrypoint.sh"]
