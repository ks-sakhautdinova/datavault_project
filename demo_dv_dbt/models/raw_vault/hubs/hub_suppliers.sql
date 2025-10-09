{{
    config(
        materialized='incremental'
        ,incremental_strategy='append'
    )
}}

SELECT 
    {{ generate_hash_key('stg.supplier_id', 'OrganicNevaVP') }} AS supplier_key
    ,stg.supplier_id
    ,'OrganicNevaVP' AS record_source
    ,GETDATE() AS load_date
FROM {{ ref('stg_dim_suppliers') }} stg

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ this }} hub
    WHERE hub.supplier_key = {{ generate_hash_key('stg.supplier_id', 'OrganicNevaVP') }}
)
{% endif %}