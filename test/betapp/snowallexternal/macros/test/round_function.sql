{% macro round_function(n1, scale=2) %}
ROUND({{n1}}, {{scale}})
{% endmacro %}

 