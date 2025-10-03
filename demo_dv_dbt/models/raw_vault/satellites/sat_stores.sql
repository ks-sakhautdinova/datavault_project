{{
    config(
        materialized='incremental'
        ,incremental_strategy='append'
    )
}}

SELECT 
    {{ generate_hash_key('stg.store_id', 'OrganicNevaVP') }} AS store_key
    ,HASHBYTES('MD5', CONCAT_WS('||'
        ,CAST(stg.name AS NVARCHAR(250))
        ,CAST(stg.store_number AS NVARCHAR(250))
        ,CAST(stg.store_format AS NVARCHAR(250))
        ,CAST(stg.store_area AS NVARCHAR(250))
        ,CAST(stg.responsible_manager AS NVARCHAR(250))
        ,'OrganicNevaVP'
    )) AS hash_diff
    ,stg.name
    ,stg.store_number
    ,stg.store_format
    ,stg.store_area
    ,stg.responsible_manager
    ,'OrganicNevaVP' AS record_source
    ,GETDATE() AS load_date
FROM {{ ref('stg_dim_stores') }} stg

{% if is_incremental() %}
AND NOT EXISTS (
    SELECT 1 
    FROM {{ this }} sat
    WHERE sat.store_key = {{ generate_hash_key('stg.store_id', 'OrganicNevaVP') }}
        AND sat.hash_diff = HASHBYTES('MD5', CONCAT_WS('||'
            ,CAST(stg.name AS NVARCHAR(250))
            ,CAST(stg.store_number AS NVARCHAR(250))
            ,CAST(stg.store_format AS NVARCHAR(250))
            ,CAST(stg.store_area AS NVARCHAR(250))
            ,CAST(stg.responsible_manager AS NVARCHAR(250))
            ,'OrganicNevaVP'
        ))
)
{% endif %}