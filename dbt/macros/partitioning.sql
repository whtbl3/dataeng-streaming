{# Iceberg partition spec for a model. The CI target runs on Trino's memory connector, which has
   no partitions, so it gets none (the SQL under test is the same). #}
{% macro partitioned_by(column) -%}
    {{ return(none if target.name == 'ci' else {'partitioning': "ARRAY['" ~ column ~ "']"}) }}
{%- endmacro %}

{# Rewrite whole days with one strategy on both engines (spec 009): dbt-athena has only `merge`
   for Iceberg (no `delete+insert`), so a pre-hook deletes the days the run rewrites and the model
   then merges. The days: the closing day (close), or every day not closed yet (intraday
   aggregate). Nothing to delete before the table exists. #}
{% macro clear_days(which) -%}
    {%- set existing = adapter.get_relation(this.database, this.schema, this.identifier) if execute else none -%}
    {%- if existing is none -%}
        select 1
    {%- elif var('mode') == 'close' -%}
        delete from {{ this }} where local_date = date '{{ var("day") }}'
    {%- elif which == 'open' -%}
        delete from {{ this }}
        where local_date not in (select local_date from {{ ref('closed_days') }})
    {%- else -%}
        select 1
    {%- endif -%}
{%- endmacro %}
