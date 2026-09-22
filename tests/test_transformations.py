import pandas as pd
import pytest

from include.transformations import (
    DataQualityError, aggregate_daily_sales, clean_orders, run_quality_checks,
)


@pytest.fixture
def raw():
    return pd.read_csv("data/orders_sample.csv")


def test_clean_removes_duplicates_and_normalizes(raw):
    df = clean_orders(raw)
    assert df["order_id"].is_unique
    assert set(df["region"]) == {"MIDWEST", "WEST", "EAST", "UNKNOWN"}


def test_aggregate(raw):
    daily = aggregate_daily_sales(clean_orders(raw))
    row = daily[(daily["region"] == "MIDWEST")].iloc[0]
    assert row["total_orders"] == 2
    assert round(row["total_revenue"], 2) == 139.93


def test_quality_checks_pass(raw):
    run_quality_checks(aggregate_daily_sales(clean_orders(raw)))


def test_quality_checks_fail_on_empty():
    with pytest.raises(DataQualityError):
        run_quality_checks(pd.DataFrame(columns=["order_date", "region", "total_revenue"]))
