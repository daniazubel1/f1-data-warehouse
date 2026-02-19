
with source as (
    select * from {{ source('f1_raw', 'raw_circuits') }}
)
select
    "circuitId" as circuit_ref,
    "url",
    "circuitName" as circuit_name,
    "Location.country" as country,
    "Location.locality" as locality,
    "Location.lat"::float as lat,
    "Location.long"::float as lng
from source
