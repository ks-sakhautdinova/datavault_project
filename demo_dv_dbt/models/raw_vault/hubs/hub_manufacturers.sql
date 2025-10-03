{{
    config(
        materialized='incremental'
        ,incremental_strategy='append'
    )
}}

SELECT 
    {{ generate_hash_key('stg.manufacturer_id', 'OrganicNevaVP') }} AS manufacturer_key
    ,stg.manufacturer_id
    ,'OrganicNevaVP' AS record_source
    ,GETDATE() AS load_date
FROM {{ ref('stg_dim_manufacturers') }} stg

{% if is_incremental() %}
AND NOT EXISTS (
    SELECT 1 
    FROM {{ this }} hub
    WHERE hub.manufacturer_key = {{ generate_hash_key('stg.manufacturer_id', 'OrganicNevaVP') }}
)
{% endif %}