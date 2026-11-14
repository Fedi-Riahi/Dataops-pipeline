select
    cast(y.county_fips || '-' || cast(y.year as varchar) as varchar) as county_year_key,
    y.county_fips,
    d.county_name,
    y.year,
    y.yield_per_acre,
    w.precip_season_mm,
    w.precip_summer_mm,
    w.temp_season_avg_c,
    w.temp_summer_avg_c,
    w.solar_season_mj
from {{ ref('stg_nass_yield') }} y
join {{ ref('dim_county') }} d on d.county_fips = y.county_fips
join {{ ref('int_weather_county_year') }} w
  on w.county_fips = y.county_fips and w.year = y.year
where y.commodity = 'corn'
  and w.season_days >= 180