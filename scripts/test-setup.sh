#!/bin/bash

set -e

mkdir -p ~/.spark/data
cd docker/test

LOCAL_UID="$(id -u)" LOCAL_GID="$(id -g)" docker compose up -d
while [[ $(docker container inspect -f "{{ .State.Health.Status }}" spark-thrift) != 'healthy' ]]; do
  echo "Waiting for spark-thrift to become healthy..."
  sleep 1
done
