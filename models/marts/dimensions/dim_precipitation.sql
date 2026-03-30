{{ config(materialized='table') }}

select distinct
    {{ hash_key(['precipitation']) }} as id,
    precipitation
from {{ ref('stg_wildlife_strike') }}
where precipitation is not null
