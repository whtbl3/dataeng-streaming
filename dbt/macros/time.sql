{# Business day and local time (BR-4): every KPI day is the Asia/Ho_Chi_Minh calendar day. #}
{% macro local_date(ts) -%}
    cast(at_timezone({{ ts }}, '{{ var("local_tz") }}') as date)
{%- endmacro %}

{# Minutes between two timestamps, b - a, with seconds as a fraction. #}
{% macro minutes_between(a, b) -%}
    (date_diff('millisecond', {{ a }}, {{ b }}) / 60000e0)
{%- endmacro %}

{# A ~150 m area around a point (NFR-6): Bing tile quadkey at zoom 18. Trino and Athena v3 both
   have the Bing tile functions; another engine needs its own branch here. #}
{% macro area_of(lat, lon) -%}
    bing_tile_quadkey(bing_tile_at({{ lat }}, {{ lon }}, 18))
{%- endmacro %}
