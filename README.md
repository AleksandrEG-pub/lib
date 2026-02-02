### Basic crud application for different types of storages, formats, operations 

Contains only scripts with manual launch. 
No api exist.

### Installation
```
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .
```

### Database

Database(pg postgres:17.5) is located in docker (port 10452). To launch database run
```
./start-database.sh
```
Database configuration 
```
./.env
```

### Project stucture

Project split on modules:
- week 3
  - library
- week 4
  - star
  - data_vault
  - partitions
- week 5
  - clickhouse
- week 6
  - s3_upload
- week 7
  - spark_upload
- week 8
  - kafka_pipeline
- week 9
  - airflow_pipeline

Main execution scripts: 
```
python ./src/library/__main__.py
python ./src/data_warehouse/star/__main__.py
python ./src/data_warehouse/data_vault/__main__.py
python ./src/data_warehouse/partition/__main__.py
python ./src/clickhouse/__main__.py
python ./src/s3_upload/__main__.py
python ./src/spark_upload/__main__.py
python ./src/kafka_pipeline/__main__.py
python ./src/airflow_pipeline/__main__.py
```

### Week 3. Data generation

> Modules generate data on start. 
> 
> Data might not be able to run twice due to constraints in database.

Sql scripts located in following directories:
- library
  - ./src/library/persistence/sql
- star
  - ./src/data_warehouse/star/sql
- data_vault
  - ./src/data_warehouse/data_vault/sql
- partition
  - ./src/data_warehouse/partition/sql


### Erd, week 4
Star:
![Star](./erd/star.png "Star")


Data vault:
![Star](./erd/data_vault.png "Star")


### Clickhouse, week 5

Clickhouse (clickhouse:25.11.3.54-jammy) is located in docker (port 10453, 10454). To launch run
```
./start-clickhouse.sh
```
Clickhouse configuration 
```
./src/clickhouse/clickhouse.env
```

Clickhouse module presented as a flow in file ./src/clickhouse/log_analytic.py.

First created a table 'web_logs'. 
Then it populated with random data for 10mil rows.
Then different analytic queries are performed in 2 steps, before and after optimization.
Optimization for different query is different.

Execution log of the 'log_analytic.py' script will show exact statistic for queries, such as:
- exetution time
- consumed memory
- total retrieved rows
- total bytes

### Grafana, week 5

```
./start-grafana.sh
```
Gran configuration is in directory 
```
./grafana
```
Contains default user:
- login: admin
- password: admin

Contains preconfigured datasource for clickhouse on http://clickhouse:9000
Contains preconfigured dashboards:
- web_logs: custom with basic info about web_logs table
- ClickHouse - Data Analysis: default clickhouse configuration
- ClickHouse - Query Analysis: default clickhouse configuration


### S3, week 6

S3 provider is seaweedfs. Started in docker with s3 API on http://seaweedfs:8333 or http://localhost:10456
```
./start-s3.sh
```
Admin ui can be started with command:
```
docker compose exec -d seaweedfs /usr/bin/weed admin
```
Admin ui will be available on http://seaweedfs:23646 or http://localhost:10457

s3_upload module does following:
- creates products tables in postgres
- populate few products
- load these products from postgres to s3 in bucket root as products.parquet
- load same products from postgres to s3 in bucket as Iceberg structure under iceberg_warehouse dir
- Result are described in ./src/s3/upload/results.txt

### Spark, week 7

Required ports:
- 10452 - potgres
- 10456 - s3
- 10457 - s3 ui, launch script look in [S3, week 6]
- 10458 - spark master ui, awailable by default

Spark presented as cluster of master + worker.
Cluster UI is available, on http://localhost:8080

To work with s3 spark requires jars:
- hadoop-aws-3.4.1.jar - api s3, used by spark
- bundle-2.24.6.jar - aws sdk, imeplementation of api

To connect to postgres spark requires sql Driver jar:
- postgresql-42.7.9.jar

Before start docker compose services, need to build spark image with extra jars:
```
./build_spark_image.sh
```

Then start required sevices:
```
./start-database.sh && ./start-s3.sh && ./start-spark.sh
```

Spark application executed in extra container:
```
./start-spark.app.sh
```

Spark configuration is in 
```
./env/spark.env
```

Spark application.
Before spark application product data located in ./src/spark_upload/data/products.csv loaded to s3 bucket into 'products.parquet'

Spark application:
- loads 'products.parquet'
- renames columns accordning to sql scheme: ./src/spark_upload/init-tables.sql
- add missing in init dataset column 'created_at'
- append table in postgres with transformed data

Result of uploading can be observed by:
```
docker compose exec database-lib bash
psql --user it_one
select * from products;
```

Spark is a solution for distributed processing. Especially effective on big datasets. Solves problem of case when data is bigger than available memory on simgle machine. Spark has built in fault taulerence, which will try to recover from failures of worker nodes.
It would require big effort to achieve level of data processing management to implement similar to spark features in plain python. 
However use of spark requires additional hardware resources, setup costs and knowledge of spark platform.


### Kafka, week 8
Required ports:
- 10452 - potgres
- 10458 - spark master ui, awailable by default

Kafka presented as single node cluster.

Start required sevices:
```
./start-database.sh && ./start-spark.sh && ./start-kafka.sh
```

Spark application executed in extra container:
```
./start-kafka-pipeline-app.sh
```

Kafka configuration is in 
```
./env/kafka.env
./env/kafka_broker.env
```

For convinience, topic created automatically, ttl set to 5 minutes

Module executes as a spark application in spark container.
Kafka-pipeline application:
- creates tables from ./sql/init-tables.sql in postgres: 'flights', 'flights_upload'
- populates table flight from data in ./data/flights.csv
- python-kafka move data from table 'flights' to kafka topic 'it-one'
- spark application moves data from topic 'it-one' to table 'flights_upload'
- failed messages written to dead letter queue 'it-one.dlq'

Result of uploading can be observed by:
```
# postgres
docker compose exec database-lib bash
psql --user it_one
select * from flights;
select * from flights_upload;

# kafka
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server http://localhost:9092 --topic it-one --group console --from-beginning
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server http://localhost:9092 --topic it-one.dlq --group console --from-beginning
```