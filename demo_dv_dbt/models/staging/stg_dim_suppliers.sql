{{ config(
    materialized='table'
) }}

SELECT 
    supplier_id
    ,name
    ,legal_name
    ,inn
    ,address
    ,phone
FROM {{ source('organic_neva_vp', 'lasmart_dim_suppliers') }}

