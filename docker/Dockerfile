FROM python:3.10.16-slim AS builder

# Increase PIP read timeout to 120 seconds to avoid timeout issues
ENV PIP_DEFAULT_TIMEOUT=120
ENV UV_LINK_MODE=copy

# install build tools
RUN apt-get update && \
    apt-get install -y build-essential curl libsasl2-dev libkrb5-dev git pkg-config libmariadb-dev && \
    apt-get clean

# Installing featurebyte dependencies
WORKDIR /app
COPY . /app
RUN pip install uv==0.7.3 && uv sync --all-groups --all-extras

RUN uv build && uv run pip install dist/featurebyte-*.whl


FROM python:3.10.16-slim AS runner

# Add runtime user and group and set permissions appropriately
RUN groupadd -g 1000 runnergroup -o
RUN useradd -g 1000 -u 1000 -o -M -d /app -r runner

RUN apt-get update && \
    apt-get install -y libsasl2-dev libsasl2-modules-gssapi-mit wkhtmltopdf gosu curl libkrb5-dev krb5-user && \
    apt-get clean

WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:${PATH}"

## Copy script
COPY ./docker/entrypoint.sh /docker-entrypoint.sh
COPY ./docker/migration.py /scripts/migration.py
RUN chmod +x /docker-entrypoint.sh
RUN chown -R 1000:1000 /app
