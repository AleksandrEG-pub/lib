#!/bin/sh

export DEBUG=1
docker compose --file ./docker-compose.yaml up -d --build --force-recreate spark-upload
