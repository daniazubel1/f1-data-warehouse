
with source as (
    select * from {{ source('f1_raw', 'raw_races') }}
)
select
    concat("season", '-', "round") as race_id,
    "season"::int as race_year,
    "round"::int as race_round,
    "circuitId" as circuit_ref,
    "raceName" as name,
    "date"::date as race_date,
    "time"::time as race_time,
    "url"
from source
