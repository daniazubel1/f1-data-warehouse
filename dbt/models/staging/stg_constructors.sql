
with source as (
    select * from {{ source('f1_raw', 'raw_constructors') }}
)
select
    "constructorId" as constructor_ref,
    "url",
    "name",
    "nationality"
from source
