"""Daily sales ETL: extract raw orders -> clean/aggregate -> validate -> load to Postgres."""
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.transformations import clean_orders, aggregate_daily_sales, run_quality_checks

SOURCE_FILE = Path(__file__).parents[1] / "data" / "orders_sample.csv"
TARGET_TABLE = "analytics.daily_sales"

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
}


@dag(
    dag_id="sales_etl",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "sales"],
)
def sales_etl():
    @task
    def extract() -> str:
        df = pd.read_csv(SOURCE_FILE)
        return df.to_json(orient="records")

    @task
    def transform(raw_json: str) -> str:
        df = pd.read_json(raw_json, orient="records")
        daily = aggregate_daily_sales(clean_orders(df))
        return daily.to_json(orient="records", date_format="iso")

    @task
    def quality_check(daily_json: str) -> str:
        df = pd.read_json(daily_json, orient="records")
        run_quality_checks(df)
        return daily_json

    @task
    def load(daily_json: str) -> int:
        df = pd.read_json(daily_json, orient="records")
        hook = PostgresHook(postgres_conn_id="postgres_default")
        hook.run(
            """
            CREATE SCHEMA IF NOT EXISTS analytics;
            CREATE TABLE IF NOT EXISTS analytics.daily_sales (
                order_date DATE, region TEXT, total_orders INT, total_revenue NUMERIC(12,2),
                PRIMARY KEY (order_date, region)
            );
            """
        )
        dates = tuple(pd.to_datetime(df["order_date"]).dt.date.astype(str).unique())
        hook.run(f"DELETE FROM {TARGET_TABLE} WHERE order_date = ANY(%s)", parameters=(list(dates),))
        rows = list(df[["order_date", "region", "total_orders", "total_revenue"]].itertuples(index=False, name=None))
        hook.insert_rows(TARGET_TABLE, rows, target_fields=["order_date", "region", "total_orders", "total_revenue"])
        return len(rows)

    load(quality_check(transform(extract())))


sales_etl()
