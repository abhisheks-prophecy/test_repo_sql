{% macro qa_concat_function_main_shared(param_1_col, param_2_col) %}
concat({{param_1_col}}, {{param_2_col}})
{% endmacro %}

 