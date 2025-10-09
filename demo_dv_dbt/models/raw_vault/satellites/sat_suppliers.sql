{{
    config(
        materialized='incremental'
        ,incremental_strategy='append'
    )
}}

SELECT 
    {{ generate_hash_key('stg.supplier_id', 'OrganicNevaVP') }} AS supplier_key
    ,HASHBYTES('MD5', CONCAT_WS('||'
        ,CAST(stg.name AS NVARCHAR(250))
        ,CAST(stg.legal_name AS NVARCHAR(250))
        ,CAST(stg.inn AS NVARCHAR(250))
        ,CAST(stg.address AS NVARCHAR(250))
        ,CAST(stg.phone AS NVARCHAR(250))
        ,'OrganicNevaVP'
    )) AS hash_diff
    ,stg.name
    ,stg.legal_name
    ,stg.inn
    ,stg.address
    ,stg.phone
    ,'OrganicNevaVP' AS record_source
    ,GETDATE() AS load_date
FROM {{ ref('stg_dim_suppliers') }} stg


{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ this }} sat
    WHERE sat.supplier_key = {{ generate_hash_key('stg.supplier_id', 'OrganicNevaVP') }}
        AND sat.hash_diff = HASHBYTES('MD5', CONCAT_WS('||'
            ,CAST(stg.name AS NVARCHAR(250))
            ,CAST(stg.legal_name AS NVARCHAR(250))
            ,CAST(stg.inn AS NVARCHAR(250))
            ,CAST(stg.address AS NVARCHAR(250))
            ,CAST(stg.phone AS NVARCHAR(250))
            ,'OrganicNevaVP'
        ))
)
{% endif %}