FROM python:3.9

RUN apt-get update && \
    apt-get upgrade -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y \
    openjdk-17-jdk \
    build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && cd /tmp

COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock

RUN pip install --upgrade pip poetry \
    && poetry config virtualenvs.create false \
    && poetry export -f requirements.txt --without-hashes --output requirements.txt \
    && pip wheel --no-cache-dir --wheel-dir=/home/model-server/deps -r requirements.txt

RUN pip install --no-index --no-cache /home/model-server/deps/* \
    && rm -rf /home/model-server/deps

COPY ./config.properties /home/model-server/config.properties
COPY ./log4j2.xml /home/model-server/log4j2.xml
COPY ./transformer.mar /home/model-server/model-store/transformer.mar
COPY ./dockerd-entrypoint.sh /home/model-server/dockerd-entrypoint.sh

RUN chmod +x /home/model-server/dockerd-entrypoint.sh

EXPOSE 8080 8081 8082 7070 7071

ENTRYPOINT ["/home/model-server/dockerd-entrypoint.sh"]
CMD ["serve"]
