# Need to route to different cpu arch
FROM arm64v8/python:3.8-slim

ARG USER=user
ARG UID=1000

ARG FEATUREBYTE_NP_PASSWORD
ARG FEATUREBYTE_REPOSITORY=us-central1-python.pkg.dev/vpc-host-nonprod-xa739-xz970/featurebyte-pypi
ARG FEATUREBYTE_NP_USERNAME=_json_key_base64
ENV FEATUREBYTE_REPOSITORY=${FEATUREBYTE_REPOSITORY?missing_repository}
ENV POETRY_HTTP_BASIC_FEATUREBYTE_NP_USERNAME=${FEATUREBYTE_NP_USERNAME?missing_username}
ENV POETRY_HTTP_BASIC_FEATUREBYTE_NP_PASSWORD=${FEATUREBYTE_NP_PASSWORD?missing_featurebyte_serviceaccount_base64_key}

# Add non-root user
RUN adduser --uid $UID --disabled-password --gecos '' $USER

# install build tools
RUN apt-get update && \
    apt-get install -y build-essential netcat && \
    apt-get clean

RUN pip install --extra-index-url "https://${POETRY_HTTP_BASIC_FEATUREBYTE_NP_USERNAME}:${POETRY_HTTP_BASIC_FEATUREBYTE_NP_PASSWORD}@${FEATUREBYTE_REPOSITORY}/simple/" poetry==1.2.0rc99

# Extend PATH to use pip-installed tools from shell (like alembic)
ENV PATH="${PATH}:/home/$USER/.local/bin"
WORKDIR /home/$USER/app

# Copy project files
COPY poetry.lock pyproject.toml /home/$USER/app/
COPY ./docker/entrypoint-featurebyte.sh /scripts/entrypoint.sh
COPY ./docker/migration.py              /scripts/migration.py

# Install requirements
RUN poetry config virtualenvs.create false && \
    poetry install --only main --extras "server" --no-interaction --no-ansi && \
    rm -rf ~/.cache/pypoetry/artifacts && \
    rm -rf ~/.cache/pypoetry/cache

COPY ./featurebyte /home/$USER/app/featurebyte
COPY ./dist        /home/$USER/app/dist

RUN pip install databricks-sql-connector==2.1.0 &&\
    pip install pyarrow==8.0.0 && \
    pip install jupyterlab && \
    pip install --no-index -f /home/$USER/app/dist featurebyte

RUN chown -R $USER:$USER /home/$USER/ &&\
    unset POETRY_HTTP_BASIC_FEATUREBYTE_NP_PASSWORD
USER $USER
WORKDIR /home/$USER/app

ENTRYPOINT ["/bin/bash", "/scripts/entrypoint.sh"]
