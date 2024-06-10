{{ config(
    materialized='table',
    unique_key='LOCATION_ID'
) }}

with location_data as (
    select
        LATITUDE,
        LONGITUDE,
        CITY,
        COUNTRY
    from {{ ref('location_parameters') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['LATITUDE','LONGITUDE']) }} as LOCATION_ID,
    LATITUDE,
    LONGITUDE,
    CITY,
    COUNTRY
from location_data
