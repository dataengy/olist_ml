{#
    Сырая таблица Olist для staging.

    duck: сырой слой — dbt seeds (схема raw), поэтому ref(): dbt знает, что staging
          строится ПОСЛЕ seed. С source() порядок не гарантирован — на пустой БД
          staging/тесты источников могли стартовать раньше, чем seed создаст таблицу
          («Table with name orders does not exist», гонка в dbt build).
    ch:   сырые таблицы грузятся извне dbt → source('olist_raw', ...).

    Условный ref/source разрешается при парсинге под активный таргет.
#}
{% macro raw_table(name) -%}
    {%- if target.type == 'duckdb' -%}
        {{ ref(name) }}
    {%- else -%}
        {{ source('olist_raw', name) }}
    {%- endif -%}
{%- endmacro %}
