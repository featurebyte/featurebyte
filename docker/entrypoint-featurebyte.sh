#!/bin/bash

while ! nc -zvw10 mongo-rs 27022 1>/dev/null 2>&1; do echo "Waiting for upstream mongo1 to start"; sleep 2; done; echo "mongo1 listening"
while ! nc -zvw10 mongo-rs 27021 1>/dev/null 2>&1; do echo "Waiting for upstream mongo2 to start"; sleep 2; done; echo "mongo2 listening"

# perform database migration
sleep 3 # To allow mongodb container to create replicaset if necessary
echo "Performing migration"
PYTHONPATH=$PWD python /scripts/migration.py

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
    --no-browser &

JUPYTER_PID=$1

# Soft link featurebyte config file
ln -s /app/.featurebyte/config.yaml /app/.featurebyte/notebook/config.yaml

echo "Writing additional samples in ~/.featurebyte/notebook/samples"
mkdir -p /app/.featurebyte/notebook/samples  # This will not replace
cp -nr /samples/* /app/.featurebyte/notebook/samples/  # Copy without replacement

echo "Featurebyte Beta Server is Running"
echo "┌────────────────────────┬─────────────────────────────────────────────────────────────┐"
echo "│ Service                │ Connection Details                                          │"
echo "├────────────────────────┼─────────────────────────────────────────────────────────────┤"
echo "│ featurebyte-backend    │ http://localhost:8088/                                      │"
echo "├────────────────────────┼─────────────────────────────────────────────────────────────┤"
echo "│ featurebyte-docs       │ http://localhost:8089/                                      │"
echo "├────────────────────────┼─────────────────────────────────────────────────────────────┤"
echo "│ jupyterlab-notebook    │ http://localhost:8090/lab/tree/samples/instructions.ipynb   │"
echo "└────────────────────────┴─────────────────────────────────────────────────────────────┘"
echo ""
echo "Persistent Configuration Files"
echo "┌─────────────────────────┬────────────────────────────┐"
echo "│ General Config Location │ ~/.featurebyte/config.yaml │"
echo "├─────────────────────────┼────────────────────────────┤"
echo "│ MongoDB Files           │ ~/.featurebyte/data        │"
echo "├─────────────────────────┼────────────────────────────┤"
echo "│ Jupyter Notebooks       │ ~/.featurebyte/notebook    │"
echo "└─────────────────────────┴────────────────────────────┘"
echo ""

# start featurebyte service (Long running process, CTRL+C to stop)
uvicorn featurebyte.app:app --host=$API_HOST --port=$API_PORT --workers=$WORKERS --timeout-keep-alive=300 --log-level=$LOG_LEVEL

# Stop jupyter
kill -TERM ${JUPYTER_PID}
