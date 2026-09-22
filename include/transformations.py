"""Transformation and data-quality logic, kept free of Airflow imports so it is easy to test."""
import pandas as pd


class DataQualityError(Exception):
    pass


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [c.strip().lower() for c in out.columns]
    out = out.dropna(subset=["order_id", "order_date"])
    out = out.drop_duplicates(subset=["order_id"], keep="last")
    out["order_date"] = pd.to_datetime(out["order_date"]).dt.date
    out["region"] = out["region"].fillna("UNKNOWN").str.strip().str.upper()
    out["quantity"] = pd.to_numeric(out["quantity"], errors="coerce").fillna(0).astype(int)
    out["unit_price"] = pd.to_numeric(out["unit_price"], errors="coerce").fillna(0.0)
    out["amount"] = (out["quantity"] * out["unit_price"]).round(2)
    return out


def aggregate_daily_sales(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["order_date", "region"], as_index=False)
        .agg(total_orders=("order_id", "nunique"), total_revenue=("amount", "sum"))
        .sort_values(["order_date", "region"])
        .reset_index(drop=True)
    )


def run_quality_checks(df: pd.DataFrame) -> None:
    if df.empty:
        raise DataQualityError("No rows produced by transform")
    if df[["order_date", "region"]].isnull().any().any():
        raise DataQualityError("Null values found in key columns")
    if (df["total_revenue"] < 0).any():
        raise DataQualityError("Negative revenue found")
    if df.duplicated(subset=["order_date", "region"]).any():
        raise DataQualityError("Duplicate (order_date, region) keys")
