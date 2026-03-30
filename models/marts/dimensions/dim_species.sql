{{ config(materialized='table') }}

select distinct
    {{ hash_key(['species_id', 'species_name']) }} as id,
    species_id,
    species_name
from {{ ref('stg_wildlife_strike') }}
where species_id is not null
