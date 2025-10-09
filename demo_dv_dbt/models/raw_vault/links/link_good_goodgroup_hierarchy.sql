{{
    config(
        materialized='incremental'
        ,incremental_strategy='append'
    )
}}

SELECT 
    {{ generate_hash_key('stg.parent_group', 'OrganicNevaVP') }} AS parent_goodgroup_key
    ,{{ generate_hash_key('stg.goodgroup_id', 'OrganicNevaVP') }} AS child_goodgroup_key
    ,HASHBYTES('MD5', CONCAT_WS('||'
        ,CAST(stg.parent_group AS NVARCHAR(250))
        ,CAST(stg.goodgroup_id AS NVARCHAR(250))
        ,'OrganicNevaVP'
    )) AS gg_hierarchy_key
    ,'OrganicNevaVP' AS record_source
    ,GETDATE() AS load_date
FROM {{ ref('stg_dim_goodgroups') }} stg

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ this }} lnk
    WHERE lnk.gg_hierarchy_key = HASHBYTES('MD5', CONCAT_WS('||'
        ,CAST(stg.parent_group AS NVARCHAR(250))
        ,CAST(stg.goodgroup_id AS NVARCHAR(250))
        ,'OrganicNevaVP'
    ))
)
{% endif %}