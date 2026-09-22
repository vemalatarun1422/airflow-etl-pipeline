# Airflow ETL Pipeline

An end-to-end **ETL pipeline orchestrated with Apache Airflow**. It extracts raw order data, cleans and validates it with Pandas, runs data quality checks, and loads an analytics-ready table into PostgreSQL (swap in Snowflake/BigQuery/Redshift via an Airflow connection).

## Architecture

```
 CSV / API source ──► extract ──► transform ──► quality_check ──► load ──► PostgreSQL (analytics.daily_sales)
                     (raw)       (clean,        (row counts,      (upsert)
                                  dedupe,        nulls, ranges)
                                  aggregate)
```

## Tech Stack
Apache Airflow 2.x (TaskFlow API) · Python · Pandas · PostgreSQL · Docker Compose · pytest

## Project Structure
```
dags/sales_etl_dag.py        # Airflow DAG (daily schedule, retries, catchup off)
include/transformations.py   # Pure-Python transform + data-quality logic (unit tested)
data/orders_sample.csv       # Sample source data
tests/test_transformations.py
docker-compose.yml           # Local Airflow + Postgres
requirements.txt
```

## Run Locally
```bash
docker compose up -d
# Airflow UI: http://localhost:8080  (user: airflow / pass: airflow)
# Trigger the `sales_etl` DAG
```

## Run Tests
```bash
pip install pandas pytest
pytest -q
```

## Key Features
- Idempotent daily loads (delete-then-insert per `ds` partition)
- Data quality gate that fails the run on empty data, null keys, or negative amounts
- Retries with exponential backoff and task-level logging
- Transform logic separated from the DAG so it can be unit tested
