{{ config(
    materialized='incremental',
    unique_key='WEATHER_DATA_ID'
) }}

select
    {{ dbt_utils.generate_surrogate_key(['LATITUDE','LONGITUDE','VALIDDATE']) }} as WEATHER_DATA_ID,
    {{ dbt_utils.generate_surrogate_key(['VALIDDATE']) }} as DATE_ID,
    {{ dbt_utils.generate_surrogate_key(['LATITUDE','LONGITUDE']) }} as LOCATION_ID,
    {{ dbt_utils.generate_surrogate_key(['WEATHER_CONDITION_ID']) }} as WEATHER_CONDITION_ID,
    TEMPERATURE,
    {{ dbt_utils.generate_surrogate_key(['TEMPERATURE_UNIT']) }} as TEMPERATURE_UNIT_ID,
    PRECIPITATION,
    {{ dbt_utils.generate_surrogate_key(['PRECIPITATION_UNIT']) }} as PRECIPITATION_UNIT_ID,
    HUMIDITY,
    {{ dbt_utils.generate_surrogate_key(['HUMIDITY_UNIT']) }} as HUMIDITY_UNIT_ID,
    PRESSURE,
    {{ dbt_utils.generate_surrogate_key(['PRESSURE_UNIT']) }} as PRESSURE_UNIT_ID,
    DEW_POINT,
    {{ dbt_utils.generate_surrogate_key(['DEW_POINT_UNIT']) }} as DEW_POINT_UNIT_ID,
    WIND_SPEED,
    {{ dbt_utils.generate_surrogate_key(['WIND_SPEED_UNIT']) }} as WIND_SPEED_UNIT_ID,
    CLOUD_COVER,
    {{ dbt_utils.generate_surrogate_key(['CLOUD_COVER_UNIT']) }} as CLOUD_COVER_UNIT_ID,
    LOAD_TIMESTAMP
from {{ ref('weather_raw_p') }}

