select county_fips, obs_date, count(*) as n
from {{ ref('stg_nasa_power') }}
group by 1, 2
having count(*) > 1