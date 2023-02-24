#!/bin/bash

while ! nc -zvw10 mongo-rs 27022 1>/dev/null 2>&1; do echo "Waiting for upstream mongo1 to start"; sleep 2; done; echo "mongo1 listening"
while ! nc -zvw10 mongo-rs 27021 1>/dev/null 2>&1; do echo "Waiting for upstream mongo2 to start"; sleep 2; done; echo "mongo2 listening"

# perform database migration
sleep 3 # To allow mongodb container to create replicaset if necessary

echo "Performing migration"
PYTHONPATH=$PWD python /scripts/migration.py

echo "Persistent Configuration Files"
echo "┌─────────────────────────┬────────────────────────────┐"
echo "│ General Config Location │ ~/.featurebyte/config.yaml │"
echo "└─────────────────────────┴────────────────────────────┘"
echo ""
echo "Featurebyte Service"
echo "┌────────────────────────┬────────────────────────────────────────────────────────────────────────┐"
echo "│ Service                │ Connection Details                                                     │"
echo "├────────────────────────┼────────────────────────────────────────────────────────────────────────┤"
echo "│ featurebyte-api        │ http://localhost:8088/api/v1                                           │"
echo "└────────────────────────┴────────────────────────────────────────────────────────────────────────┘"
echo ""

# start featurebyte service (Long running process, CTRL+C to stop)
uvicorn featurebyte.app:app --host=$API_HOST --port=$API_PORT --workers=$WORKERS --timeout-keep-alive=300 --log-level=$LOG_LEVEL
