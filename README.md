## Task 1. ETL Pipeline Implementation

* To run job locally, we can execute below command from the directory where main.py is present
  ```shell
  poetry install
  ```

* then execute
  ```shell
  poetry shell
  ```

* to execute our main app execute below command
  ```shell
  python main.py \
    --source ./data/transaction.csv \
    --destination ./data/out.parquet
  ```

* to execute test cases run below command from poetry shell:
  ```shell
  pytest tests
  ```

### Bonus
* We can execute bonus assignment by:
```shell
docker compose build
docker compose run etl python main.py \
  --source /opt/data/transaction.csv \
  --database warehouse \
  --table customers
```

After this we should be able to query the aggregated data from PostgreSQL (assuming the PostgreSQL instance is called `db` in `docker-compose`):

```shell
docker compose exec db psql --user postgres -d warehouse \
  -c 'select * from customers limit 10'
```

## Task 2. System Architecture

Attached file - ETL_architecture.pdf