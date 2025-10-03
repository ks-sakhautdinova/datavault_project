{{ config(
    materialized='table'
) }}

SELECT 
    good_id
    ,name
    ,manufacturer_id
    ,type
    ,group_id
    ,price_group
    ,vat
    ,ef2_id
FROM {{ source('organic_neva_vp', 'lasmart_dim_goods') }}

