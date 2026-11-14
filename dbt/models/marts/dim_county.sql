select county_fips, county_name, lat, lon
from {{ ref('counties') }}