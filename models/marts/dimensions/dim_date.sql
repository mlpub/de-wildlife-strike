{{ config(materialized='table') }}

with distinct_dates as (

    select distinct
        date(
            cast(incident_year as int64),
            cast(incident_month as int64),
            cast(incident_day as int64)
        ) as full_date
    from {{ ref('stg_wildlife_strike') }}
    where incident_year is not null
      and incident_month is not null
      and incident_day is not null

)

select
    {{ hash_key(['full_date']) }} as id,
    full_date,
    extract(year from full_date) as year,
    extract(month from full_date) as month,
    extract(day from full_date) as day,
    extract(dayofweek from full_date) as day_of_week,
    format_date('%B', full_date) as month_name,
    extract(quarter from full_date) as quarter,
    extract(dayofweek from full_date) in (1, 7) as is_weekend
from distinct_dates
