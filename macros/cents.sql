{% macro cents_to_dollars(column_name, decimal_places=23, some_other=2) %}
round(1.0 * {{column_name}} / 100, {{decimal_places}})
{% endmacro %}

 {% macro select_all234(name='customers') %}

select * from {{name}}
{% endmacro %}

 