#!/bin/sh

echo 'starting dbt'
python -m dbt_star init
current_dir=$(pwd)

cd src/dbt_star/star_schema
dbt deps
dbt snapshot --profiles-dir .
dbt run --profiles-dir .

cd "$current_dir"
python -m dbt_star update

cd src/dbt_star/star_schema
dbt snapshot --profiles-dir .
dbt run --profiles-dir .