select
    county_fips,
    cast(date as date) as obs_date,
    temp_c,
    precip_mm,
    solar_mj_m2
from {{ source('raw', 'nasa_power_daily') }}
