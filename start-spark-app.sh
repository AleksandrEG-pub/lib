#!/bin/sh

docker compose --file ./docker-compose.yaml up -d --build --force-recreate spark-upload
