{% macro test_macro(c_int=10, c_long=100, c_decimal=4.4545) %}
{{c_int}}+{{c_long}}+{{c_decimal}}
{% endmacro %}