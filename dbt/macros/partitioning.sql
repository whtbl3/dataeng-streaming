{# Iceberg partition spec for a model. The CI target runs on Trino's memory connector, which has
   no partitions, so it gets none (the SQL under test is the same). #}
{% macro partitioned_by(column) -%}
    {{ return(none if target.name == 'ci' else {'partitioning': "ARRAY['" ~ column ~ "']"}) }}
{%- endmacro %}
