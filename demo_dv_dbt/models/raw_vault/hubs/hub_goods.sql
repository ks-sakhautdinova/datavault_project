{{
    config(
        materialized='incremental'
        ,incremental_strategy='append'
    )
}}

SELECT 
    {{ generate_hash_key('stg.good_id', 'OrganicNevaVP') }} AS good_key
    ,stg.good_id
    ,'OrganicNevaVP' AS record_source
    ,GETDATE() AS load_date
FROM {{ ref('stg_dim_goods') }} stg

{% if is_incremental() %}
AND NOT EXISTS (
    SELECT 1 
    FROM {{ this }} hub
    WHERE hub.good_key = {{ generate_hash_key('stg.good_id', 'OrganicNevaVP') }}
)
{% endif %}

