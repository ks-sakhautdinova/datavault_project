{{
    config(
        materialized='incremental'
        ,incremental_strategy='append'
    )
}}

SELECT 
    {{ generate_hash_key('stg.manufacturer_id', 'OrganicNevaVP') }} AS manufacturer_key
    ,HASHBYTES('MD5', CONCAT_WS('||'
        ,CAST(stg.name AS NVARCHAR(250))
        ,'OrganicNevaVP'
    )) AS hash_diff
    ,stg.name
    ,'OrganicNevaVP' AS record_source
    ,GETDATE() AS load_date
FROM {{ ref('stg_dim_manufacturers') }} stg

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ this }} sat
    WHERE sat.manufacturer_key = {{ generate_hash_key('stg.manufacturer_id', 'OrganicNevaVP') }}
        AND sat.hash_diff = HASHBYTES('MD5', CONCAT_WS('||'
            ,CAST(stg.name AS NVARCHAR(250))
            ,'OrganicNevaVP'
        ))
)
{% endif %}