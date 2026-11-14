import io
import zipfile

import pandas as pd
import requests

URL = ("https://www2.census.gov/geo/docs/maps-data/data/gazetteer/"
       "2023_Gazetteer/2023_Gaz_counties_national.zip")

r = requests.get(URL, timeout=60)
r.raise_for_status()
z = zipfile.ZipFile(io.BytesIO(r.content))
df = pd.read_csv(z.open(z.namelist()[0]), sep="\t", dtype={"GEOID": str})
df.columns = df.columns.str.strip()

ia = df[df["USPS"] == "IA"]
out = pd.DataFrame({
    "county_fips": ia["GEOID"],
    "county_name": ia["NAME"].str.replace(" County", "", regex=False),
    "lat": ia["INTPTLAT"],
    "lon": ia["INTPTLONG"],
})
assert len(out) == 99, f"expected 99 counties, got {len(out)}"
out.to_csv("ingestion/counties.csv", index=False)
print("wrote", len(out), "counties")