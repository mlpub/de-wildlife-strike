{{ config(materialized='table') }}

select distinct
    {{ hash_key(['warning_issued']) }} as id,
    warning_issued
from {{ ref('stg_wildlife_strike') }}
where warning_issued is not null
