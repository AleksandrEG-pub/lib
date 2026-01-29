#!/bin/sh

./build_spark_image.sh && \
docker compose --file ./docker-compose.yaml up -d spark-master spark-worker
