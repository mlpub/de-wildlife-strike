{{ config(
    materialized='table',
    partition_by={
        "field": "incident_date",
        "data_type": "date"
    },
    cluster_by=["airport_id", "operator_id", "species_id"]
) }}

with
s as (

    select *
    from {{ ref('stg_wildlife_strike') }}

)

select
    d_date.full_date as incident_date,
    d_date.id as date_id,
    d_airport.id as airport_id,
    d_operator.id as operator_id,
    d_aircraft.id as aircraft_id,
    d_species.id as species_id,
    d_visibility.id as visibility_id,
    d_precipitation.id as precipitation_id,
    d_phase.id as flight_phase_id,
    d_warning.id as warning_id,
    d_impact.id as flight_impact_id,
    d_species_qty.id as species_quantity_id,

    s.record_id,

    s.height,
    s.speed,
    s.distance,
    s.fatalities,
    s.injuries,

    s.aircraft_damage,
    s.radome_strike,
    s.radome_damage,
    s.windshield_strike,
    s.windshield_damage,
    s.nose_strike,
    s.nose_damage,
    s.engine1_strike,
    s.engine1_damage,
    s.engine2_strike,
    s.engine2_damage,
    s.engine3_strike,
    s.engine3_damage,
    s.engine4_strike,
    s.engine4_damage,
    s.engine_ingested,
    s.propeller_strike,
    s.propeller_damage,
    s.wing_or_rotor_strike,
    s.wing_or_rotor_damage,
    s.fuselage_strike,
    s.fuselage_damage,
    s.landing_gear_strike,
    s.landing_gear_damage,
    s.tail_strike,
    s.tail_damage,
    s.lights_strike,
    s.lights_damage,
    s.other_strike,
    s.other_damage,

    s.batch_id

from s
join {{ ref('dim_date') }} d_date
    on d_date.full_date = date(
        cast(s.incident_year as int64),
        cast(s.incident_month as int64),
        cast(s.incident_day as int64)
    )
join {{ ref('dim_airport') }} d_airport
    on d_airport.airport_id = s.airport_id
   and d_airport.airport_name = s.airport_name
   and d_airport.state = s.state
   and d_airport.faa_region = s.faa_region
join {{ ref('dim_operator') }} d_operator
    on d_operator.operator_id = s.operator_id
   and d_operator.operator_name = s.operator_name
join {{ ref('dim_aircraft') }} d_aircraft
    on d_aircraft.aircraft = s.aircraft
   and d_aircraft.aircraft_type = s.aircraft_type
   and d_aircraft.aircraft_make = s.aircraft_make
   and d_aircraft.aircraft_model = s.aircraft_model
   and coalesce(d_aircraft.aircraft_mass, -1) = coalesce(s.aircraft_mass, -1)
   and d_aircraft.engine_make = s.engine_make
   and d_aircraft.engine_model = s.engine_model
   and coalesce(d_aircraft.engines, -1) = coalesce(s.engines, -1)
   and d_aircraft.engine_type = s.engine_type
join {{ ref('dim_species') }} d_species
    on d_species.species_id = s.species_id
   and d_species.species_name = s.species_name
join {{ ref('dim_visibility') }} d_visibility
    on d_visibility.visibility = s.visibility
join {{ ref('dim_precipitation') }} d_precipitation
    on d_precipitation.precipitation = s.precipitation
join {{ ref('dim_flight_phase') }} d_phase
    on d_phase.flight_phase = s.flight_phase
join {{ ref('dim_warning') }} d_warning
    on d_warning.warning_issued = s.warning_issued
join {{ ref('dim_flight_impact') }} d_impact
    on d_impact.flight_impact = s.flight_impact
join {{ ref('dim_species_quantity') }} d_species_qty
    on d_species_qty.species_quantity = s.species_quantity
