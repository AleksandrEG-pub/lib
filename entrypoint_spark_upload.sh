#!/bin/bash

if [ "$DEBUG" = "true" ] || [ "$DEBUG" = "1" ]; then
    echo "Starting spark_upload app with debugger on port 5678"
    exec python -m debugpy \
        --listen 0.0.0.0:5678 \
        --wait-for-client \
        -m spark_upload "$@"
else
    echo "Starting spark_upload app"
    exec python -m spark_upload "$@"
fi
