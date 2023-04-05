{% macro test_function_all(inp) %}
concat(
  'test', {{inp}}
)
{% endmacro %}