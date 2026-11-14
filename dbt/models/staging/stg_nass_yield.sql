select
    county_fips,
    lower(commodity) as commodity,
    year,
    yield_per_acre
from {{ source('raw', 'nass_yield') }}
