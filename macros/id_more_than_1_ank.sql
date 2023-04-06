{% macro id_more_than_1_ank(table_name, id_min = '2') %}
    select * from {{ table_name }} where id > {{ id_min }}
{% endmacro %}