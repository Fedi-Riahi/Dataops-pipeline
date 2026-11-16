import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import pandas as pd
import requests
from prefect import flow, get_run_logger, task

from ingestion import modis, nasa_power, nass
from ingestion.common import archive_parquet, load_contract, upsert, validate


def notify(msg: str) -> None:
    url = os.getenv("ALERT_WEBHOOK_URL")
    if url:
        requests.post(url, json={"text": msg, "content": msg}, timeout=10)


def alert(flow, flow_run, state):
    notify(f"Pipeline '{flow.name}' run {flow_run.name} ended in {state.name}")


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

def _load_ndvi(df: pd.DataFrame, key: str) -> int:
    if df.empty:
        return 0
    contract = load_contract("modis_ndvi")
    validate(df, contract)
    archive_parquet(df, key)
    return upsert(df, "modis_ndvi", contract["primary_key"])


@task(retries=2, retry_delay_seconds=60)
def ingest_ndvi(days_back: int = 32):
    counties = pd.read_csv("ingestion/counties.csv", dtype={"county_fips": str})
    end = date.today()
    start = end - timedelta(days=days_back)
    frames = []
    for r in counties.itertuples():
        frames.append(modis.fetch_ndvi(r.county_fips, r.lat, r.lon,
                                       modis.modis_date(start), modis.modis_date(end)))
        time.sleep(0.5)
    return _load_ndvi(pd.concat(frames, ignore_index=True), f"raw/modis_ndvi/{end}.parquet")


def _fetch_county_ndvi(fips: str, lat: float, lon: float, years: list[int]) -> pd.DataFrame:
    frames = []
    for y in years:
        for a, b in [(97, 161), (177, 241), (257, 289)]:  # Apr-Oct, 5/5/3 dates
            frames.append(modis.fetch_ndvi(fips, lat, lon, f"A{y}{a:03d}", f"A{y}{b:03d}"))
    return pd.concat(frames, ignore_index=True)


@flow(name="iowa-ndvi-backfill")
def ndvi_backfill(start_year: int = 2023, workers: int = 4):
    log = get_run_logger()
    years = list(range(start_year, date.today().year + 1))
    counties = pd.read_csv("ingestion/counties.csv", dtype={"county_fips": str})
    failed = []
    # Downloads run in parallel; DuckDB writes stay sequential (single writer).
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_fetch_county_ndvi, r.county_fips, r.lat, r.lon, years): r
            for r in counties.itertuples()
        }
        for fut in as_completed(futures):
            r = futures[fut]
            try:
                n = _load_ndvi(fut.result(), f"raw/modis_ndvi/backfill/{r.county_fips}.parquet")
                log.info("%s %s: %s rows", r.county_fips, r.county_name, n)
            except Exception:  # noqa: BLE001
                log.error("%s %s failed, rerun to fill it", r.county_fips, r.county_name)
                failed.append(r.county_fips)
    if failed:
        log.warning("failed counties: %s", failed)

@task
def check_freshness():
    subprocess.run(
        ["dbt", "source", "freshness", "--project-dir", "dbt", "--profiles-dir", "dbt"],
        check=True,
    )

@task
def run_dbt():
    subprocess.run(["dbt", "build", "--project-dir", "dbt", "--profiles-dir", "dbt"], check=True)


@flow(name="iowa-daily", on_failure=[alert])
def iowa_daily():
    log = get_run_logger()
    log.info("weather rows: %s", ingest_weather())
    log.info("yield rows: %s", ingest_yield())
    log.info("ndvi rows: %s", ingest_ndvi())
    check_freshness()
    run_dbt()


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
