{{ config(
    materialized='table'
) }}

SELECT 
    manufacturer_id
    ,name
FROM {{ source('organic_neva_vp', 'lasmart_dim_manufacturers') }}

