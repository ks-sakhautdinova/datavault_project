{{
    config(
        materialized='incremental'
        ,incremental_strategy='append'
        ,unique_key=['link_key', 'load_date']
    )
}}

SELECT 
    HASHBYTES('MD5', CONCAT_WS('||'
        ,CAST(stg.ID_CHEQUE AS NVARCHAR(250))
        ,CAST(stg.ID_Store AS NVARCHAR(250))
        ,CAST(stg.ID_GOODS AS NVARCHAR(250))
        ,CAST(stg.ID_SUPPLIER AS NVARCHAR(250))
        ,'OrganicNevaVP'
    )) AS sale_key
    ,HASHBYTES('MD5', CONCAT_WS('||'
        ,CAST(stg.CHEQUE_TYPE AS NVARCHAR(250))
        ,CAST(stg.QUANTITY AS NVARCHAR(250))
        ,CAST(stg.PRICE AS NVARCHAR(250))
        ,CAST(stg.SUMM_DISCOUNT AS NVARCHAR(250))
        ,CAST(stg.SUMM_CHEQUE_ITEM AS NVARCHAR(250))
        ,CAST(stg.SUM_ACC AS NVARCHAR(250))
        ,CAST(stg.COST AS NVARCHAR(250))
        ,CAST(stg.Discount_Name AS NVARCHAR(250))
        ,'OrganicNevaVP'
    )) AS hash_diff
    ,stg.CHEQUE_TYPE
    ,stg.QUANTITY
    ,stg.PRICE
    ,stg.SUMM_DISCOUNT
    ,stg.SUMM_CHEQUE_ITEM
    ,stg.SUM_ACC
    ,stg.COST
    ,stg.Discount_Name
    ,'OrganicNevaVP' AS record_source
    ,GETDATE() AS load_date
FROM {{ ref('stg_fact_cheques') }} stg

{% if is_incremental() %}
AND NOT EXISTS (
    SELECT 1 
    FROM {{ this }} sat
    WHERE sat.sale_key = HASHBYTES('MD5', CONCAT_WS('||'
        ,CAST(stg.ID_CHEQUE AS NVARCHAR(250))
        ,CAST(stg.ID_Store AS NVARCHAR(250))
        ,CAST(stg.ID_GOODS AS NVARCHAR(250))
        ,CAST(stg.ID_SUPPLIER AS NVARCHAR(250))
        ,'OrganicNevaVP'
    ))
        AND sat.hash_diff = HASHBYTES('MD5', CONCAT_WS('||'
            ,CAST(stg.CHEQUE_TYPE AS NVARCHAR(250))
            ,CAST(stg.QUANTITY AS NVARCHAR(250))
            ,CAST(stg.PRICE AS NVARCHAR(250))
            ,CAST(stg.SUMM_DISCOUNT AS NVARCHAR(250))
            ,CAST(stg.SUMM_CHEQUE_ITEM AS NVARCHAR(250))
            ,CAST(stg.SUM_ACC AS NVARCHAR(250))
            ,CAST(stg.COST AS NVARCHAR(250))
            ,CAST(stg.Discount_Name AS NVARCHAR(250))
            ,'OrganicNevaVP'
        ))
)
{% endif %}