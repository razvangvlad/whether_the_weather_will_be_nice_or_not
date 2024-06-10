{{ config(
    materialized='incremental',
    unique_key='DATE_ID'
) }}

with weather_data as (
    select distinct
        VALIDDATE AS DATE,
        extract(year from VALIDDATE) as YEAR,
        extract(month from VALIDDATE) as MONTH,
        extract(day from VALIDDATE) as DAY,
        to_char(VALIDDATE, 'Day') as DAY_OF_WEEK
    from {{ ref('weather_raw_p') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['DATE']) }} as DATE_ID,
    DATE,
    YEAR,
    MONTH,
    DAY,
    DAY_OF_WEEK
from weather_data
