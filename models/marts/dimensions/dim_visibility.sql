{{ config(materialized='table') }}

select distinct
    {{ hash_key(['visibility']) }} as id,
    visibility
from {{ ref('stg_wildlife_strike') }}
where visibility is not null
