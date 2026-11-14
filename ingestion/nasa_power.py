import pandas as pd
import requests

URL = "https://power.larc.nasa.gov/api/temporal/daily/point"
PARAMS = "T2M,PRECTOTCORR,ALLSKY_SFC_SW_DWN"


def fetch_county(fips: str, lat: float, lon: float, start: str, end: str) -> pd.DataFrame:
    """start/end as YYYYMMDD."""
    r = requests.get(
        URL,
        params={"parameters": PARAMS, "community": "AG", "latitude": lat,
                "longitude": lon, "start": start, "end": end, "format": "JSON"},
        timeout=60,
    )
    r.raise_for_status()
    p = r.json()["properties"]["parameter"]
    df = pd.DataFrame(p).rename_axis("date").reset_index()
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d").dt.date
    df = df.rename(columns={"T2M": "temp_c", "PRECTOTCORR": "precip_mm",
                            "ALLSKY_SFC_SW_DWN": "solar_mj_m2"})
    df = df.replace(-999, pd.NA)  # NASA POWER missing-value flag
    df.insert(0, "county_fips", fips)
    return df
