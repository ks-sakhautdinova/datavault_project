{{
    config(
        materialized='incremental'
        ,incremental_strategy='append'
    )
}}

SELECT 
    {{ generate_hash_key('stg.ID_CHEQUE', 'OrganicNevaVP') }} AS cheque_key
    ,{{ generate_hash_key('stg.ID_Store', 'OrganicNevaVP') }} AS store_key
    ,{{ generate_hash_key('stg.ID_GOODS', 'OrganicNevaVP') }} AS good_key
    ,{{ generate_hash_key('stg.ID_SUPPLIER', 'OrganicNevaVP') }} AS supplier_key
    ,HASHBYTES('MD5', CONCAT_WS('||'
        ,CAST(stg.ID_CHEQUE AS NVARCHAR(250))
        ,CAST(stg.ID_Store AS NVARCHAR(250))
        ,CAST(stg.ID_GOODS AS NVARCHAR(250))
        ,CAST(stg.ID_SUPPLIER AS NVARCHAR(250))
        ,'OrganicNevaVP'
    )) AS sale_key
    ,stg.dt AS transaction_date
    ,'OrganicNevaVP' AS record_source
    ,GETDATE() AS load_date
FROM {{ ref('stg_fact_cheques') }} stg

{% if is_incremental() %}
AND NOT EXISTS (
    SELECT 1 
    FROM {{ this }} lnk
    WHERE lnk.sale_key = HASHBYTES('MD5', CONCAT_WS('||'
        ,CAST(stg.ID_CHEQUE AS NVARCHAR(250))
        ,CAST(stg.ID_Store AS NVARCHAR(250))
        ,CAST(stg.ID_GOODS AS NVARCHAR(250))
        ,CAST(stg.ID_SUPPLIER AS NVARCHAR(250))
        ,'OrganicNevaVP'
    ))
)
{% endif %}