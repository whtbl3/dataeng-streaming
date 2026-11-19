{# Business day and local time (BR-4): every KPI day is the Asia/Ho_Chi_Minh calendar day. #}
{% macro local_date(ts) -%}
    cast(at_timezone({{ ts }}, '{{ var("local_tz") }}') as date)
{%- endmacro %}

{# Minutes between two timestamps, b - a, with seconds as a fraction. #}
{% macro minutes_between(a, b) -%}
    (date_diff('millisecond', {{ a }}, {{ b }}) / 60000e0)
{%- endmacro %}
