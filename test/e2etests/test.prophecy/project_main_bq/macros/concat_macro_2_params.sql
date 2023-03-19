{% macro concat_macro_2_string_bool_params(p_string='hello sir', p_bool=True) %}
concat({{p_string}}, {{p_bool}})
{% endmacro %}

 