{{ config(materialized='table') }}

select distinct
    {{ hash_key([
        'aircraft',
        'aircraft_type',
        'aircraft_make',
        'aircraft_model',
        'coalesce(aircraft_mass, -1)',
        'engine_make',
        'engine_model',
        'coalesce(engines, -1)',
        'engine_type'
    ]) }} as id,
    aircraft,
    aircraft_type,
    aircraft_make,
    aircraft_model,
    aircraft_mass,
    engine_make,
    engine_model,
    engines,
    engine_type
from {{ ref('stg_wildlife_strike') }}
where aircraft is not null
