{% macro test_macro_parametrized(p_string, p_int=12, p_bool=true, p_unknown='i am the unknown') %}
concat({{p_string}}, {{p_int}}, {{p_bool}}, {{p_unknown}})
{% endmacro %}

 