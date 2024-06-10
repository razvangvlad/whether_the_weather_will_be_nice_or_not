{{ config(
    materialized='table',
    unique_key='UNIT_ID'
) }}

with weather_data as (
    select distinct
        DATAITEM_UNIT as UNIT,
        UNIT_NAME as UNIT_DESCRIPTION
    from {{ ref('data_parameters') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['UNIT']) }} as UNIT_ID,
    UNIT,
    UNIT_DESCRIPTION
from weather_data
