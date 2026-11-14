import os

import duckdb
import streamlit as st

st.title("Iowa corn yield platform")
path = os.getenv("WAREHOUSE_PATH", "/data/warehouse.duckdb")

if not os.path.exists(path):
    st.info("Warehouse not created yet. Wait for the first pipeline run.")
    st.stop()

con = duckdb.connect(path, read_only=True)
st.subheader("Data health")
for table, col in [("nasa_power_daily", "date"), ("nass_yield", "year")]:
    try:
        n, last = con.execute(f"select count(*), max({col}) from raw.{table}").fetchone()
        st.metric(f"raw.{table}", f"{n} rows", f"latest {last}")
    except duckdb.Error:
        st.warning(f"raw.{table} not loaded yet")
# TODO: business questions, NDVI, yield estimate
