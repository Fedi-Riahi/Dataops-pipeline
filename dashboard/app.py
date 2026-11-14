import os
from datetime import date

import duckdb
import pandas as pd
import streamlit as st

PATH = os.getenv("WAREHOUSE_PATH", "/data/warehouse.duckdb")

st.set_page_config(page_title="Iowa corn yield", layout="wide")
st.title("Iowa corn yield platform")

if not os.path.exists(PATH):
    st.info("Warehouse not created yet. Wait for the first pipeline run.")
    st.stop()


@st.cache_data(ttl=300)
def q(sql: str) -> pd.DataFrame:
    con = duckdb.connect(PATH, read_only=True)
    try:
        return con.execute(sql).df()
    finally:
        con.close()


try:
    fct = q("select * from fct_county_year")
    health = q("""
        select count(*) as rows, count(distinct county_fips) as counties,
               max(date) as latest from raw.nasa_power_daily""")
    yields = q("select count(*) as rows, max(year) as latest_year from raw.nass_yield")
except duckdb.Error as e:
    st.error(f"Warehouse not ready: {e}")
    st.stop()

tab_data, tab_yield, tab_health = st.tabs(["Yield vs weather", "County view", "Data health"])

with tab_data:
    st.subheader("Average corn yield by year (bu/acre)")
    st.line_chart(fct.groupby("year")["yield_per_acre"].mean())
    driver = st.selectbox("Weather driver", [
        "precip_summer_mm", "temp_summer_avg_c", "precip_season_mm",
        "temp_season_avg_c", "solar_season_mj"])
    st.scatter_chart(fct, x=driver, y="yield_per_acre", color="year")
    st.caption(f"Correlation with yield: {fct[driver].corr(fct['yield_per_acre']):.2f}")

with tab_yield:
    county = st.selectbox("County", sorted(fct["county_name"].unique()))
    c = fct[fct["county_name"] == county].sort_values("year").set_index("year")
    st.line_chart(c[["yield_per_acre"]])
    st.dataframe(c.drop(columns=["county_year_key", "county_fips", "county_name"]))

with tab_health:
    latest = pd.Timestamp(health["latest"].iloc[0]).date()
    lag = (date.today() - latest).days
    a, b, c3, d = st.columns(4)
    a.metric("Weather rows", f"{health['rows'].iloc[0]:,}")
    b.metric("Counties", int(health["counties"].iloc[0]))
    c3.metric("Latest weather date", str(latest), f"{lag} days ago", delta_color="inverse")
    d.metric("Yield rows", f"{yields['rows'].iloc[0]:,}", f"up to {yields['latest_year'].iloc[0]}")
    if lag > 5:
        st.error("Weather data is stale (more than 5 days old).")
    else:
        st.success("Weather data is fresh.")