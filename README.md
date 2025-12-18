### Basic crud Library application. 

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

Database(pg postgres:17.5) is located in docker. To launch database run
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

Main execution scripts: 
```
python ./src/library/__main__.py
python ./src/data_warehouse/star/__main__.py
python ./src/data_warehouse/data_vault/__main__.py
python ./src/data_warehouse/partition/__main__.py
```

### Data generation

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


### Erd
Star:
![Star](./erd/star.png "Star")


Data vault:
![Star](./erd/data_vault.png "Star")
