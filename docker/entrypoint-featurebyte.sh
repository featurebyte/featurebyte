#!/bin/bash

while ! nc -zvw10 mongo-rs 27022 1>/dev/null 2>&1; do echo "Waiting for upstream mongo1 to start"; sleep 2; done; echo "mongo1 listening"
while ! nc -zvw10 mongo-rs 27021 1>/dev/null 2>&1; do echo "Waiting for upstream mongo2 to start"; sleep 2; done; echo "mongo2 listening"

# perform database migration
echo "Performing migration"
PYTHONPATH=$PWD python /scripts/migration.py >/dev/null 2>&1

# Start notebook
echo "Starting jupyter notebook"
echo "Storing notebooks in directory: ~/.featurebyte/notebook"
if [[ ! -d /app/.featurebyte/notebook ]]; then mkdir /app/.featurebyte/notebook; fi
jupyter-lab \
    --notebook-dir=/app/.featurebyte/notebook \
    --port=8090 \
    --NotebookApp.token='' \
    --NotebookApp.password='' \
    --NotebookApp.allow_origin='*' \
    --NotebookApp.ip='0.0.0.0' \
    --allow-root \
    --no-browser > /dev/null 2>&1 &

# Start serving docs
echo "Starting mkdocs server"
PYTHONPATH=/app/docs/extensions FB_GENERATE_FULL_DOCS=1 poetry run mkdocs serve --no-livereload --dev-addr 0.0.0.0:8089 > /dev/null 2>&1 &

while ! nc -zvw10 localhost 8089 1>/dev/null 2>&1; do echo "Waiting for local mkdocs server to start"; sleep 2; done; echo "mkdocs server is running"


echo "Featurebyte Beta Server is Running"
echo "┌────────────────────────┬──────────────────────────┐"
echo "│Service                 │Connection Details        │"
echo "├────────────────────────┼──────────────────────────┤"
echo "│featurebyte-backend     │localhost:8088            │"
echo "├────────────────────────┼──────────────────────────┤"
echo "│featurebyte-swagger-docs│localhost:8089            │"
echo "├────────────────────────┼──────────────────────────┤"
echo "│jupyterlab-notebook     │localhost:8090            │"
echo "└────────────────────────┴──────────────────────────┘"
echo ""
echo "Persistent Configuration Files"
echo "┌────────────────────────┬──────────────────────────┐"
echo "│General Config Location │~/.featurebyte            │"
echo "├────────────────────────┼──────────────────────────┤"
echo "│MongoDB Files           │~/.featurebyte/data       │"
echo "├────────────────────────┴──────────────────────────┤"
echo "│Jupyter Notebooks       │~/.featurebyte/notebook   │"
echo "└────────────────────────┴──────────────────────────┘"
echo ""

# start featurebyte service
uvicorn featurebyte.app:app --host=$API_HOST --port=$API_PORT --workers=$WORKERS --timeout-keep-alive=300 --log-level=$LOG_LEVEL
