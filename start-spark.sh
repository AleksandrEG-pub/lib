#!/bin/sh

docker compose --file ./docker-compose.yaml up -d spark-master spark-worker
