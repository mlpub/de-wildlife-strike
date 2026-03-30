{{ config(materialized='table') }}

select distinct
    {{ hash_key(['airport_id', 'airport_name', 'state', 'faa_region']) }} as id,
    airport_id,
    airport_name,
    state,
    faa_region
from {{ ref('stg_wildlife_strike') }}
where airport_id is not null
