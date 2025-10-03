{{
    config(
        materialized='incremental'
        ,incremental_strategy='append'
    )
}}

SELECT 
    {{ generate_hash_key('stg.good_id', 'OrganicNevaVP') }} AS good_key
    ,{{ generate_hash_key('stg.manufacturer_id', 'OrganicNevaVP') }} AS manufacturer_key
    ,HASHBYTES('MD5', CONCAT_WS('||'
        ,CAST(stg.good_id AS NVARCHAR(250))
        ,CAST(stg.manufacturer_id AS NVARCHAR(250))
        ,'OrganicNevaVP'
    )) AS good_manufacturer_key
    ,'OrganicNevaVP' AS record_source
    ,GETDATE() AS load_date
FROM {{ ref('stg_dim_goods') }} stg

{% if is_incremental() %}
AND NOT EXISTS (
    SELECT 1 
    FROM {{ this }} lnk
    WHERE lnk.good_manufacturer_key = HASHBYTES('MD5', CONCAT_WS('||'
        ,CAST(stg.good_id AS NVARCHAR(250))
        ,CAST(stg.manufacturer_id AS NVARCHAR(250))
        ,'OrganicNevaVP'
    ))
)
{% endif %}
