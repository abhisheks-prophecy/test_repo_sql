{% macro test_not_null(model, column_name) %}
select count(*) from {{ model }} where {{ column_name }} is null
{% endmacro %}