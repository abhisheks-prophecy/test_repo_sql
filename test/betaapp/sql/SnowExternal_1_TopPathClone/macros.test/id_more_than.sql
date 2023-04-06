{% macro id_more_than(table=customers, id_min=2) %}
select * from {{ table }} where id > {{ id_min }}
{% endmacro %}

 