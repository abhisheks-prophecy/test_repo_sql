{% macro test_function_all1(inp, inp2=2) %}
concat(
  'test', {{inp}}, {{inp2}}
)
{% endmacro %}