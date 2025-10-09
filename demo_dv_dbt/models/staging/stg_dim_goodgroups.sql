{{ config(
    materialized='table'
) }}

--check pipeline
SELECT 
    goodgroup_id
    ,name
    ,goodgroup
    ,category
    ,parent_group
    ,[group]
    ,child_group
    --index_tree
FROM {{ source('organic_neva_vp', 'lasmart_dim_goodgroups') }}

