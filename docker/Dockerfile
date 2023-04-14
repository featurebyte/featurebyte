FROM python:3.8-slim AS builder

ARG FEATUREBYTE_REPOSITORY=us-central1-python.pkg.dev/vpc-host-nonprod-xa739-xz970/featurebyte-pypi
ARG FEATUREBYTE_NP_USERNAME=_json_key_base64
ARG FEATUREBYTE_NP_PASSWORD
ENV FEATUREBYTE_REPOSITORY=${FEATUREBYTE_REPOSITORY?missing_repository}
ENV FEATUREBYTE_NP_USERNAME=${FEATUREBYTE_NP_USERNAME?missing_username}
ENV FEATUREBYTE_NP_PASSWORD=${FEATUREBYTE_NP_PASSWORD?missing_featurebyte_serviceaccount_base64_key}

# install build tools
RUN apt-get update && \
    apt-get install -y build-essential curl libsasl2-dev && \
    apt-get clean

WORKDIR /app
COPY pyproject.toml poetry.lock /app/

# Installing featurebyte dependencies
RUN pip install poetry==1.3.1 && \
    python -m venv /opt/venv && \
    poetry config virtualenvs.create false && \
    poetry config http-basic.featurebyte_np ${FEATUREBYTE_NP_USERNAME} ${FEATUREBYTE_NP_PASSWORD} && \
    . /opt/venv/bin/activate && \
    poetry install -n --sync --extras=server

# Moving featurebyte source code
COPY ./featurebyte /app/featurebyte
COPY ./README.md /app/README.md
COPY ./LICENSE /app/LICENSE

RUN . /opt/venv/bin/activate && \
    poetry build && \
    pip install dist/*.whl


FROM python:3.8-slim AS runner

ARG USER_HOME=/app

# Add runtime user and group and set permissions appropriately
RUN groupadd -g 1000 runnergroup -o
RUN useradd -g 1000 -u 1000 -o -M -d /app -r runner

RUN apt-get update && \
    apt-get install -y libsasl2-dev wkhtmltopdf gosu curl && \
    apt-get clean

WORKDIR $USER_HOME
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

## Copy script
COPY ./docker/entrypoint.sh             /scripts/entrypoint.sh
RUN chmod +x /scripts/entrypoint.sh
RUN chown -R 1000:1000 /app

CMD ["/bin/bash", "/scripts/entrypoint.sh"]
