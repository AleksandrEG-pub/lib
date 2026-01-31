#!/bin/bash

if [ "$DEBUG" = "true" ] || [ "$DEBUG" = "1" ]; then
    echo "Starting airflow_pipeline app with debugger on port 5678"
    exec python -m debugpy \
        --listen 0.0.0.0:5678 \
        --wait-for-client \
        -m airflow_pipeline "$@"
else
    echo "Starting airflow_pipeline app"
    exec python -m airflow_pipeline "$@"
fi
