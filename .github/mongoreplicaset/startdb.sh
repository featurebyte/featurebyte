#!/bin/bash

docker-compose up -d

sleep 1

docker exec mongo /scripts/rs-init.sh
