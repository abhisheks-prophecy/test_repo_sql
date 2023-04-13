{% macro qa_concat_param_type_base(input_string='hello', int_param=10) %}
concat({{input_string}}, {{int_param}})
{% endmacro %}

 {% macro multi_macro_expressions_base(param_float=12, param_array=[1, 2, 3], param_dict=[1, 2, 3]) %}
concat({{param_float}} + {{param_array[0]}}, 'hello')
{% endmacro %}

 {% macro round_function_base(n1, scale=2) %}
ROUND({{n1}}, {{scale}})
{% endmacro %}

 {% macro qa_macro_call_another_macro_base(final_param='random data') %}
concat({{ qa_concat_macro(final_param) }}, {{final_param}})
{% endmacro %}
