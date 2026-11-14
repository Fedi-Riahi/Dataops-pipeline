import numpy as np
import pandas as pd
import requests
import time

URL = "https://modis.ornl.gov/rst/api/v1/MOD13Q1/subset"
BAND = "250m_16_days_NDVI"
SCALE = 0.0001
COLUMNS = ["county_fips", "date", "ndvi_mean", "ndvi_median", "valid_pixels", "total_pixels"]


def modis_date(d) -> str:
    return f"A{d.year}{d.timetuple().tm_yday:03d}"

def _get(params: dict, tries: int = 4) -> requests.Response:
    for i in range(tries):
        try:
            r = requests.get(URL, params=params,
                             headers={"Accept": "application/json"}, timeout=(10, 180))
            r.raise_for_status()
            return r
        except requests.RequestException:
            if i == tries - 1:
                raise
            time.sleep(30 * (i + 1))

def fetch_ndvi(fips: str, lat: float, lon: float, start: str, end: str) -> pd.DataFrame:
    """Max 10 MODIS dates per request. start/end like A2024153."""
    r = _get({"latitude": lat, "longitude": lon, "band": BAND, "startDate": start,
              "endDate": end, "kmAboveBelow": 1, "kmLeftRight": 1})
    r.raise_for_status()
    rows = []
    for s in r.json().get("subset", []):
        px = np.array(s["data"], dtype=float)
        valid = px[(px >= -2000) & (px <= 10000)]  # -3000 is the fill value
        rows.append({
            "county_fips": fips,
            "date": pd.to_datetime(s["calendar_date"]).date(),
            "ndvi_mean": valid.mean() * SCALE if len(valid) else np.nan,
            "ndvi_median": float(np.median(valid)) * SCALE if len(valid) else np.nan,
            "valid_pixels": int(len(valid)),
            "total_pixels": int(len(px)),
        })
    return pd.DataFrame(rows, columns=COLUMNS)