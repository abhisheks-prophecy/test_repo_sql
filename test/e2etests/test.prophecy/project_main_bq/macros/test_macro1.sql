{% macro sum_3_variables(c_int=10, c_long=20, c_float=10.12) %}
{{c_int}} + {{c_long}} + {{c_float}}
{% endmacro %}

 