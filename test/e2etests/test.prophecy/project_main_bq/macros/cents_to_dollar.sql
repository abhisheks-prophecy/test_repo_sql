{% macro cents_to_dollar(column_name, precision=2) %}
ROUND({{ column_name }} / 100, {{ precision }})
{% endmacro %}

 