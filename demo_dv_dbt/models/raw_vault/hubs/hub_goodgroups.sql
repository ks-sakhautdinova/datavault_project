{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

SELECT 
    {{ generate_hash_key('stg.goodgroup_id', 'OrganicNevaVP') }} AS goodgroup_key
    ,stg.goodgroup_id
    ,'OrganicNevaVP' AS record_source
    ,GETDATE() AS load_date
FROM {{ ref('stg_dim_goodgroups') }} stg

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ this }} hub
    WHERE hub.goodgroup_key = {{ generate_hash_key('stg.goodgroup_id', 'OrganicNevaVP') }}
)
{% endif %}

