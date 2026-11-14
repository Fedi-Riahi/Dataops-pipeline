import os

import boto3
import duckdb
import pandas as pd
import yaml

WAREHOUSE = os.getenv("WAREHOUSE_PATH", "/data/warehouse.duckdb")


def load_contract(name: str) -> dict:
    with open(f"contracts/{name}.yml") as f:
        return yaml.safe_load(f)


def validate(df: pd.DataFrame, contract: dict) -> None:
    """Block bad data before it reaches the warehouse. Raises ValueError."""
    errors = []
    for col, spec in contract["columns"].items():
        if col not in df.columns:
            errors.append(f"missing column {col}")
            continue
        if not spec.get("nullable", False) and df[col].isna().any():
            errors.append(f"nulls in {col}")
        if "min" in spec and (df[col].dropna() < spec["min"]).any():
            errors.append(f"{col} below {spec['min']}")
        if "max" in spec and (df[col].dropna() > spec["max"]).any():
            errors.append(f"{col} above {spec['max']}")
        if "allowed" in spec and not df[col].dropna().isin(spec["allowed"]).all():
            errors.append(f"{col} has values outside {spec['allowed']}")
    keys = contract["primary_key"]
    if all(k in df.columns for k in keys) and df.duplicated(keys).any():
        errors.append(f"duplicate keys on {keys}")
    if errors:
        raise ValueError(f"Contract '{contract['name']}' violated: {errors}")


def upsert(df: pd.DataFrame, table: str, keys: list[str]) -> int:
    """Idempotent load: re-running the same batch never creates duplicates."""
    con = duckdb.connect(WAREHOUSE)
    con.execute("create schema if not exists raw")
    con.register("batch", df)
    con.execute(f"create table if not exists raw.{table} as select * from batch where false")
    cond = " and ".join(f"raw.{table}.{k} = batch.{k}" for k in keys)
    con.execute(f"delete from raw.{table} using batch where {cond}")
    con.execute(f"insert into raw.{table} select * from batch")
    con.close()
    return len(df)


def archive_parquet(df: pd.DataFrame, key: str) -> None:
    """Keep a raw copy in S3/MinIO (data lake layer)."""
    local = "/tmp/batch.parquet"
    df.to_parquet(local, index=False)
    s3 = boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT_URL") or None)
    bucket = os.environ["S3_BUCKET"]
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)
    s3.upload_file(local, bucket, key)
