#!/bin/bash

set -e

mkdir -p ~/.spark/data
cd docker

LOCAL_UID="$(id -u)" LOCAL_GID="$(id -g)" docker compose up -d
while [[ $(docker container inspect -f "{{ .State.Health.Status }}" spark-thrift-test) != 'healthy' ]]; do
  echo "Waiting for spark-thrift to become healthy..."
  sleep 1
done

set -x
docker exec spark-thrift-test /bin/bash -c "df -h /tmp"
