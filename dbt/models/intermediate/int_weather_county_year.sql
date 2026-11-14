select
    county_fips,
    cast(extract(year from obs_date) as int) as year,
    sum(precip_mm) filter (where month(obs_date) between 4 and 9) as precip_season_mm,
    sum(precip_mm) filter (where month(obs_date) between 6 and 8) as precip_summer_mm,
    avg(temp_c)    filter (where month(obs_date) between 4 and 9) as temp_season_avg_c,
    avg(temp_c)    filter (where month(obs_date) between 6 and 8) as temp_summer_avg_c,
    sum(solar_mj_m2) filter (where month(obs_date) between 4 and 9) as solar_season_mj,
    count(*)       filter (where month(obs_date) between 4 and 9) as season_days
from {{ ref('stg_nasa_power') }}
group by 1, 2