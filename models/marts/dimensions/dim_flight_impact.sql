{{ config(materialized='table') }}

select distinct
    {{ hash_key(['flight_impact']) }} as id,
    flight_impact
from {{ ref('stg_wildlife_strike') }}
where flight_impact is not null
