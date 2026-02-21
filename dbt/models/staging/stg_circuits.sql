
with source as (
    select * from {{ source('f1_raw', 'raw_circuits') }}
)
select
    "circuitId" as circuit_ref,
    "url",
    "circuitName" as circuit_name,
    "country",
    "locality",
    "lat"::float as lat,
    "long"::float as lng
from source
