import os

import pandas as pd
import requests

URL = "https://quickstats.nass.usda.gov/api/api_GET/"

SHORT_DESC = {
    "CORN": "CORN, GRAIN - YIELD, MEASURED IN BU / ACRE",
    "SOYBEANS": "SOYBEANS - YIELD, MEASURED IN BU / ACRE",
}
def fetch_yield(commodity: str, start_year: int) -> pd.DataFrame:
    r = requests.get(
        URL,
        params={"key": os.environ["NASS_API_KEY"], "commodity_desc": commodity,
                "state_alpha": "IA", "short_desc": SHORT_DESC[commodity],
                "agg_level_desc": "COUNTY", "year__GE": start_year, "format": "JSON"},
        timeout=120,
    )
    r.raise_for_status()
    df = pd.DataFrame(r.json()["data"])
    df["county_fips"] = df["state_fips_code"] + df["county_code"]
    df["yield_per_acre"] = pd.to_numeric(df["Value"].str.replace(",", ""), errors="coerce")
    df["commodity"] = commodity
    df["year"] = df["year"].astype(int)
    return df[["county_fips", "commodity", "year", "yield_per_acre", "unit_desc"]].dropna(
        subset=["yield_per_acre"]
    ).drop_duplicates(["county_fips", "commodity", "year"])
