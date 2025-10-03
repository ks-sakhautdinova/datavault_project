/*
        Генерирует суррогатный ключ для Data Vault 
        
        Параметры:
        - business_key_column: название колонки бизнес-ключа (например 'goodgroup_id')
        - record_source: название источника как строка (например 'OrganicNevaVP')
        
        Формула: HASH(business_key || record_source)
        
        Пример вызова:
        {{ generate_hash_key('goodgroup_id', 'OrganicNevaVP') }}
        → HASH(goodgroup_id || 'OrganicNevaVP')

*/

{% macro generate_hash_key(business_key_column, record_source) %}

    HASHBYTES('MD5', CONCAT_WS('||'
        ,CAST({{ business_key_column }} AS NVARCHAR(MAX))
        ,'{{ record_source }}'
    ))
    
{% endmacro %}

