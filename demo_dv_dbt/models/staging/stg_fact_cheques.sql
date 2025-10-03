{{ config(
    materialized='table'
) }}

SELECT 
    ID_CHEQUE
    ,dt
    ,TimeID
    ,CHEQUE_TYPE
    ,QUANTITY
    ,PRICE
    ,SUMM_DISCOUNT
    ,SUMM_CHEQUE_ITEM
    ,ID_Store
    ,ID_GOODS
    ,ID_SUPPLIER
    ,SUM_ACC
    ,COST
    ,Discount_Name
FROM {{ source('organic_neva_vp', 'lasmart_fact_cheques') }}

