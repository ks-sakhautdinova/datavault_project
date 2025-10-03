{{
    config(
        materialized='incremental'
        ,incremental_strategy='append'
        ,unique_key=['goodgroup_key', 'load_date']
    )
}}

SELECT 
    {{ generate_hash_key('stg.goodgroup_id', 'OrganicNevaVP') }} AS goodgroup_key
    ,HASHBYTES('MD5', CONCAT_WS('||'
        ,CAST(stg.name AS NVARCHAR(250))
        ,CAST(stg.goodgroup AS NVARCHAR(250))
        ,CAST(stg.category AS NVARCHAR(250))
        ,CAST(stg.[group] AS NVARCHAR(250))
    )) AS hash_diff
    ,stg.name
    ,stg.goodgroup
    ,stg.category
    ,stg.[group]
    ,'OrganicNevaVP' AS record_source
    ,GETDATE() AS load_date
FROM {{ ref('stg_dim_goodgroups') }} stg

{% if is_incremental() %}
AND NOT EXISTS (
    SELECT 1 
    FROM {{ this }} sat
    WHERE sat.goodgroup_key = {{ generate_hash_key('stg.goodgroup_id', 'OrganicNevaVP') }}
        AND sat.hash_diff = HASHBYTES('MD5', CONCAT_WS('||'
            ,CAST(stg.name AS NVARCHAR(250))
            ,CAST(stg.goodgroup AS NVARCHAR(250))
            ,CAST(stg.category AS NVARCHAR(250))
            ,CAST(stg.[group] AS NVARCHAR(250))
        ))
)
{% endif %}