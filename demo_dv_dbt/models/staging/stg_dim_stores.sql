{{ config(
    materialized='table'
) }}

SELECT 
    store_id
    ,name
    ,store_number
    ,store_format
    ,store_area
    ,responsible_manager
FROM {{ source('organic_neva_vp', 'lasmart_dim_stores') }}

