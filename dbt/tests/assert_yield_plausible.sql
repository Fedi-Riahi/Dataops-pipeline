select * from {{ ref('fct_county_year') }}
where yield_per_acre not between 20 and 300