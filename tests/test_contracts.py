import pandas as pd
import pytest

from ingestion.common import load_contract, validate


def test_valid_weather_passes():
    df = pd.DataFrame({"county_fips": ["19153"], "date": ["2026-06-01"],
                       "temp_c": [21.0], "precip_mm": [3.2], "solar_mj_m2": [6.1]})
    validate(df, load_contract("nasa_power"))


def test_out_of_range_is_blocked():
    df = pd.DataFrame({"county_fips": ["19153"], "date": ["2026-06-01"],
                       "temp_c": [999.0], "precip_mm": [3.2], "solar_mj_m2": [6.1]})
    with pytest.raises(ValueError):
        validate(df, load_contract("nasa_power"))


def test_duplicates_are_blocked():
    row = {"county_fips": "19153", "date": "2026-06-01",
           "temp_c": 20.0, "precip_mm": 1.0, "solar_mj_m2": 5.0}
    with pytest.raises(ValueError):
        validate(pd.DataFrame([row, row]), load_contract("nasa_power"))
