#!/bin/sh

docker compose --file ./docker-compose.yaml up -d --build --force-recreate airflow-pipeline
