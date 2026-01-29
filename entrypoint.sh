#!/bin/bash

if [ "$DEBUG" = "true" ] || [ "$DEBUG" = "1" ]; then
    echo "Starting spark app with debugger on port 5678"
    exec /opt/venv/bin/python -m debugpy \
        --listen 0.0.0.0:5678 \
        --wait-for-client \
        -m spark_upload "$@"
else
    echo "Starting spark app"
    exec /opt/venv/bin/python -m spark_upload "$@"
fi
