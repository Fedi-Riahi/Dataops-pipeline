import os
import subprocess
import time
from datetime import date, timedelta

import pandas as pd
import requests
from prefect import flow, get_run_logger, task

from ingestion import nasa_power, nass
from ingestion.common import archive_parquet, load_contract, upsert, validate


def alert(flow, flow_run, state):
    """Failure hook: post to a webhook if configured."""
    url = os.getenv("ALERT_WEBHOOK_URL")
    msg = f"Pipeline '{flow.name}' run {flow_run.name} ended in {state.name}"
    if url:
        requests.post(url, json={"text": msg, "content": msg}, timeout=10)


@task(retries=2, retry_delay_seconds=30)
def ingest_weather(days_back: int = 7):
    counties = pd.read_csv("ingestion/counties.csv", dtype={"county_fips": str})
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=days_back)
    frames = [
        nasa_power.fetch_county(r.county_fips, r.lat, r.lon,
                                start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
        for r in counties.itertuples()
    ]
    df = pd.concat(frames, ignore_index=True)
    contract = load_contract("nasa_power")
    validate(df, contract)  # blocks bad data before loading
    archive_parquet(df, f"raw/nasa_power/{end}.parquet")
    return upsert(df, "nasa_power_daily", contract["primary_key"])


@task(retries=2, retry_delay_seconds=30)
def ingest_yield():
    df = nass.fetch_yield("CORN", int(os.getenv("START_YEAR", "2015")))
    contract = load_contract("nass_yield")
    validate(df, contract)
    archive_parquet(df, "raw/nass_yield/latest.parquet")
    return upsert(df, "nass_yield", contract["primary_key"])


@task
def run_dbt():
    subprocess.run(["dbt", "build", "--project-dir", "dbt", "--profiles-dir", "dbt"], check=True)


@flow(name="iowa-daily", on_failure=[alert])
def iowa_daily():
    log = get_run_logger()
    log.info("weather rows: %s", ingest_weather())
    log.info("yield rows: %s", ingest_yield())
    run_dbt()  # dbt tests fail -> flow fails -> alert; nothing gets published


@task(retries=3, retry_delay_seconds=60)
def backfill_county(fips: str, lat: float, lon: float, start: str, end: str):
    df = nasa_power.fetch_county(fips, lat, lon, start, end)
    contract = load_contract("nasa_power")
    validate(df, contract)
    archive_parquet(df, f"raw/nasa_power/backfill/{fips}.parquet")
    n = upsert(df, "nasa_power_daily", contract["primary_key"])
    time.sleep(1)  # be polite to the API
    return n


@flow(name="iowa-backfill")
def iowa_backfill(start_year: int = 2015):
    log = get_run_logger()
    counties = pd.read_csv("ingestion/counties.csv", dtype={"county_fips": str})
    start = f"{start_year}0101"
    end = (date.today() - timedelta(days=7)).strftime("%Y%m%d")  # POWER lags a few days
    total = 0
    for r in counties.itertuples():
        total += backfill_county(r.county_fips, r.lat, r.lon, start, end)
        log.info("%s %s done", r.county_fips, r.county_name)
    log.info("backfill rows: %s", total)

if __name__ == "__main__":
    iowa_daily.serve(name="iowa-daily-6am", cron="0 6 * * *")
