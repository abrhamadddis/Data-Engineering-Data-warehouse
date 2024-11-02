{{ config(
    materialized='table'
) }}
SELECT *
FROM {{ source('model', 'trajectory_information') }}