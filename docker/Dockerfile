FROM python:3.10.16-slim AS builder

# Increase PIP read timeout to 120 seconds to avoid timeout issues
ENV PIP_DEFAULT_TIMEOUT=120

# install build tools
RUN apt-get update && \
    apt-get install -y build-essential curl libsasl2-dev libkrb5-dev git pkg-config libmariadb-dev && \
    apt-get clean

WORKDIR /app
COPY pyproject.toml poetry.lock /app/

# Installing featurebyte dependencies
RUN pip install poetry==1.8.1 && \
    python -m venv /opt/venv && \
    poetry config virtualenvs.create false && \
    . /opt/venv/bin/activate && \
    poetry install -n --extras=server --without dev,docs,lint

# Moving featurebyte source code
COPY ./featurebyte /app/featurebyte
COPY ./README.md /app/README.md
COPY ./LICENSE /app/LICENSE

RUN . /opt/venv/bin/activate && \
    poetry build && \
    pip install dist/*.whl


FROM python:3.10.16-slim AS runner

ARG USER_HOME=/app

# Add runtime user and group and set permissions appropriately
RUN groupadd -g 1000 runnergroup -o
RUN useradd -g 1000 -u 1000 -o -M -d /app -r runner

RUN apt-get update && \
    apt-get install -y libsasl2-dev libsasl2-modules-gssapi-mit wkhtmltopdf gosu curl libkrb5-dev krb5-user && \
    apt-get clean

WORKDIR $USER_HOME
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

## Copy script
COPY ./docker/entrypoint.sh /docker-entrypoint.sh
COPY ./docker/migration.py /scripts/migration.py
RUN chmod +x /docker-entrypoint.sh
RUN chown -R 1000:1000 /app
