{{
    config(
        materialized='incremental'
        ,incremental_strategy='append'
    )
}}

SELECT 
    {{ generate_hash_key('stg.good_id', 'OrganicNevaVP') }} AS good_key
    ,HASHBYTES('MD5', CONCAT_WS('||'
        ,CAST(stg.name AS NVARCHAR(250))
        ,CAST(stg.type AS NVARCHAR(250))
        ,CAST(stg.price_group AS NVARCHAR(250))
        ,CAST(stg.vat AS NVARCHAR(250))
        ,'OrganicNevaVP'
    )) AS hash_diff
    ,stg.name
    ,stg.type
    ,stg.price_group
    ,stg.vat
    ,'OrganicNevaVP' AS record_source
    ,GETDATE() AS load_date
FROM {{ ref('stg_dim_goods') }} stg

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ this }} sat
    WHERE sat.good_key = {{ generate_hash_key('stg.good_id', 'OrganicNevaVP') }}
        AND sat.hash_diff = HASHBYTES('MD5', CONCAT_WS('||'
            ,CAST(stg.name AS NVARCHAR(250))
            ,CAST(stg.type AS NVARCHAR(250))
            ,CAST(stg.price_group AS NVARCHAR(250))
            ,CAST(stg.vat AS NVARCHAR(250))
            ,'OrganicNevaVP'
        ))
)
{% endif %}