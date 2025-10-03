/*
    Переопределение дефолтного макроса generate_schema_name
    
    По умолчанию dbt создает схемы в формате: <target_schema>_<custom_schema>
    Этот макрос убирает префикс target_schema и использует только custom_schema
    
    Пример:
    - Без макроса: dbo_staging (неправильно)
    - С макросом: staging (правильно)
*/

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

