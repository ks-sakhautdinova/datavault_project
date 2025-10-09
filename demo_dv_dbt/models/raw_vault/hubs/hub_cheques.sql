{{
    config(
        materialized='incremental'
        ,incremental_strategy='append'
    )
}}

SELECT 
    {{ generate_hash_key('stg.ID_CHEQUE', 'OrganicNevaVP') }} AS cheque_key
    ,stg.ID_CHEQUE
    ,'OrganicNevaVP' AS record_source
    ,GETDATE() AS load_date
FROM {{ ref('stg_fact_cheques') }} stg

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ this }} hub
    WHERE hub.cheque_key = {{ generate_hash_key('stg.ID_CHEQUE', 'OrganicNevaVP') }}
)
{% endif %}

