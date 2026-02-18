
with source as (
    select * from {{ source('f1_raw', 'raw_drivers') }}
)

select
    "driverId" as driver_ref,
    "url",
    "givenName" as given_name,
    "familyName" as family_name,
    "dateOfBirth" as date_of_birth,
    "nationality"
from source
