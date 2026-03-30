{{ config(materialized='table') }}

select distinct
    {{ hash_key(['species_quantity']) }} as id,
    species_quantity
from {{ ref('stg_wildlife_strike') }}
where species_quantity is not null
