with source_data as (

    select *
    from {{ source('raw', 'raw_wildlife_strike') }}

),

cleaned as (

    select
        -- incident
        `Record ID` as record_id,
        `Incident Year` as incident_year,
        `Incident Month` as incident_month,
        `Incident Day` as incident_day,

        -- operator
        coalesce(nullif(trim(`Operator ID`), ''), 'UNKNOWN') as operator_id,
        coalesce(nullif(trim(`Operator`), ''), 'UNKNOWN') as operator_name,

        -- aircraft
        coalesce(nullif(trim(`Aircraft`), ''), 'UNKNOWN') as aircraft,
        coalesce(nullif(trim(`Aircraft Type`), ''), 'UNKNOWN') as aircraft_type,
        coalesce(nullif(trim(`Aircraft Make`), ''), 'UNKNOWN') as aircraft_make,
        coalesce(nullif(trim(`Aircraft Model`), ''), 'UNKNOWN') as aircraft_model,
        `Aircraft Mass` as aircraft_mass,

        -- engine
        coalesce(nullif(trim(cast(`Engine Make` as string)), ''), 'UNKNOWN') as engine_make,
        coalesce(nullif(trim(`Engine Model`), ''), 'UNKNOWN') as engine_model,
        `Engines` as engines,
        coalesce(nullif(trim(`Engine Type`), ''), 'UNKNOWN') as engine_type,

        nullif(trim(cast(`Engine1 Position` as string)), '') as engine1_position,
        cast(
            safe_cast(nullif(trim(cast(`Engine2 Position` as string)), '') as int64)
            as string
        ) as engine2_position,
        case
            when trim(cast(`Engine3 Position` as string)) = 'CHANGE CODE' then 'C'
            when trim(cast(`Engine3 Position` as string)) = '' then null
            else trim(cast(`Engine3 Position` as string))
        end as engine3_position,
        cast(
            safe_cast(nullif(trim(cast(`Engine4 Position` as string)), '') as int64)
            as string
        ) as engine4_position,

        -- airport
        coalesce(nullif(trim(`Airport ID`), ''), 'UNKNOWN') as airport_id,
        coalesce(nullif(trim(`Airport`), ''), 'UNKNOWN') as airport_name,
        coalesce(nullif(trim(`State`), ''), 'UNKNOWN') as state,
        coalesce(nullif(trim(`FAA Region`), ''), 'UNKNOWN') as faa_region,

        -- flight info
        case
            when trim(cast(`Warning Issued` as string)) is null then 'UNKNOWN'
            when upper(trim(cast(`Warning Issued` as string))) in ('Y', 'YES', 'TRUE', 'T', '1') then 'Y'
            when upper(trim(cast(`Warning Issued` as string))) in ('N', 'NO', 'FALSE', 'F', '0') then 'N'
            when trim(cast(`Warning Issued` as string)) = '' then 'UNKNOWN'
            else 'UNKNOWN'
        end as warning_issued,

        coalesce(nullif(trim(`Flight Phase`), ''), 'UNKNOWN') as flight_phase,
        coalesce(nullif(trim(`Visibility`), ''), 'UNKNOWN') as visibility,
        coalesce(nullif(trim(`Precipitation`), ''), 'UNKNOWN') as precipitation,

        `Height` as height,
        `Speed` as speed,
        `Distance` as distance,

        -- species
        coalesce(nullif(trim(`Species ID`), ''), 'UNKNOWN') as species_id,
        coalesce(nullif(trim(`Species Name`), ''), 'UNKNOWN') as species_name,
        coalesce(nullif(trim(`Species Quantity`), ''), 'UNKNOWN') as species_quantity,

        -- impact
        coalesce(
            case
                when trim(cast(`Flight Impact` as string)) = 'ENGINE SHUT DOWN' then 'ENGINE SHUTDOWN'
                when trim(cast(`Flight Impact` as string)) = '' then null
                else trim(cast(`Flight Impact` as string))
            end,
            'UNKNOWN'
        ) as flight_impact,

        `Fatalities` as fatalities,
        `Injuries` as injuries,
        `Aircraft Damage` as aircraft_damage,

        -- strike & damage flags
        `Radome Strike` as radome_strike,
        `Radome Damage` as radome_damage,
        `Windshield Strike` as windshield_strike,
        `Windshield Damage` as windshield_damage,
        `Nose Strike` as nose_strike,
        `Nose Damage` as nose_damage,

        `Engine1 Strike` as engine1_strike,
        `Engine1 Damage` as engine1_damage,
        `Engine2 Strike` as engine2_strike,
        `Engine2 Damage` as engine2_damage,
        `Engine3 Strike` as engine3_strike,
        `Engine3 Damage` as engine3_damage,
        `Engine4 Strike` as engine4_strike,
        `Engine4 Damage` as engine4_damage,

        `Engine Ingested` as engine_ingested,

        `Propeller Strike` as propeller_strike,
        `Propeller Damage` as propeller_damage,

        `Wing or Rotor Strike` as wing_or_rotor_strike,
        `Wing or Rotor Damage` as wing_or_rotor_damage,

        `Fuselage Strike` as fuselage_strike,
        `Fuselage Damage` as fuselage_damage,

        `Landing Gear Strike` as landing_gear_strike,
        `Landing Gear Damage` as landing_gear_damage,

        `Tail Strike` as tail_strike,
        `Tail Damage` as tail_damage,

        `Lights Strike` as lights_strike,
        `Lights Damage` as lights_damage,

        `Other Strike` as other_strike,
        `Other Damage` as other_damage,

        batch_id

    from source_data
    where batch_id = '{{ var("batch_id") }}'

)

select *
from cleaned
