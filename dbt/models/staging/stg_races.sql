
with source as (
    select * from {{ source('f1_raw', 'raw_races') }}
)
select
    "raceId" as race_id,
    "year"::int as race_year,
    "round"::int as race_round,
    "circuitId" as circuit_ref,
    "name",
    "date"::date as race_date,
    "time"::time as race_time,
    "url"
from source
