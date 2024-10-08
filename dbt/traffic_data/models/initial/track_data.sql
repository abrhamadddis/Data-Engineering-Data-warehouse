{{ config(
    materialized='table'
) }}

SELECT *
FROM {{ source('model', 'traffic_information') }}