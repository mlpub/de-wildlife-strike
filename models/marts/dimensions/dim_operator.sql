{{ config(materialized='table') }}

select distinct
    {{ hash_key(['operator_id', 'operator_name']) }} as id,
    operator_id,
    operator_name
from {{ ref('stg_wildlife_strike') }}
where operator_id is not null
