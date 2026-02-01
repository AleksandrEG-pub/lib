#!/bin/sh

docker compose --file ./docker-compose.yaml up -d --scale datanode=3 datanode om scm recon s3g httpfs 
