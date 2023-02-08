#!/bin/bash

echo "Storing notebooks in directory: ~/.featurebyte/notebook"
if [[ ! -d /app/.featurebyte/notebook ]]; then mkdir -p /app/.featurebyte/notebook; fi

# Soft link featurebyte config file
if [[ ! -f /app/.featurebyte/config.yaml ]]; then echo "# featurebyte configuration file here" > /app/.featurebyte/config.yaml; fi
ln -s /app/.featurebyte/config.yaml /app/.featurebyte/notebook/config.yaml

echo "Writing additional samples in ~/.featurebyte/notebook/samples"
mkdir -p /app/.featurebyte/notebook/samples  # This will not replace
cp -nr /samples/* /app/.featurebyte/notebook/samples/  # Copy without replacement

echo "Persistent Configuration Files"
echo "┌─────────────────────────┬────────────────────────────┐"
echo "│ General Config Location │ ~/.featurebyte/config.yaml │"
echo "├─────────────────────────┼────────────────────────────┤"
echo "│ Jupyter Notebooks       │ ~/.featurebyte/notebook    │"
echo "└─────────────────────────┴────────────────────────────┘"
echo ""
echo "Featurebyte Beta Services"
echo "┌────────────────────────┬────────────────────────────────────────────────────────────────────────┐"
echo "│ Service                │ Connection Details                                                     │"
echo "├────────────────────────┼────────────────────────────────────────────────────────────────────────┤"
echo "│ featurebyte-docs       │ http://localhost:8089/                                                 │"
echo "├────────────────────────┼────────────────────────────────────────────────────────────────────────┤"
echo "│ jupyterlab-notebook    │ http://localhost:8090/lab/tree/samples/beta-testing-instructions.ipynb │"
echo "└────────────────────────┴────────────────────────────────────────────────────────────────────────┘"
echo ""

# Start notebook (Long running process, CTRL+C to stop)
echo "Starting jupyter notebook"
jupyter-lab \
    --notebook-dir=/app/.featurebyte/notebook \
    --port=8090 \
    --NotebookApp.token='' \
    --NotebookApp.password='' \
    --NotebookApp.allow_origin='*' \
    --NotebookApp.ip='0.0.0.0' \
    --allow-root \
    --no-browser
