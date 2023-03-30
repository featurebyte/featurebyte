#!/bin/bash

echo "Running migration script"
python /scripts/migration.py
echo "starting server"
uvicorn featurebyte.app:app --host=$API_HOST --port=$API_PORT --workers=$WORKERS --timeout-keep-alive=300 --log-level=$LOG_LEVEL
