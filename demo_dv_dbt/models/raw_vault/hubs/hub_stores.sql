{{
    config(
        materialized='incremental'
        ,incremental_strategy='append'
    )
}}

SELECT 
    {{ generate_hash_key('stg.store_id', 'OrganicNevaVP') }} AS store_key
    ,stg.store_id
    ,'OrganicNevaVP' AS record_source
    ,GETDATE() AS load_date
FROM {{ ref('stg_dim_stores') }} stg

{% if is_incremental() %}
AND NOT EXISTS (
    SELECT 1 
    FROM {{ this }} hub
    WHERE hub.store_key = {{ generate_hash_key('stg.store_id', 'OrganicNevaVP') }}
)
{% endif %}