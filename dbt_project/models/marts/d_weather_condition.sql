{{ config(
    materialized='table',
    unique_key='WEATHER_CONDITION_ID'
) }}

select
    {{ dbt_utils.generate_surrogate_key(['ID']) }} as WEATHER_CONDITION_ID,
    CONDITION_DESCRIPTION,
    TIME_OF_DAY
from {{ ref('weather_conditions') }}
