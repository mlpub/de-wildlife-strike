{{ config(materialized='table') }}

select distinct
    {{ hash_key(['flight_phase']) }} as id,
    flight_phase
from {{ ref('stg_wildlife_strike') }}
where flight_phase is not null
